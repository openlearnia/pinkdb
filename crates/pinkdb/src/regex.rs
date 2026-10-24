//! A small anchored regex engine for `SIMILAR TO`.
//!
//! DuckDB 2.0's `SIMILAR TO` is a *plain anchored regex*, not the SQL-standard
//! `%`/`_` pattern: `'abcx' SIMILAR TO 'abc%'` is false and `%` is a literal
//! (measured on `/tmp/duckdb-2.0.0/duckdb`). So the pattern is compiled to a
//! Thompson NFA and simulated over the text's chars — no backtracking, so a
//! hostile pattern cannot blow up (linear in text × states).
//!
//! Supported: literals, `.`, `[...]`/`[^...]` with ranges, `\x` escapes,
//! `(…)`, `|`, `*`, `+`, `?`, `{m}`/`{m,}`/`{m,n}`, and the `^`/`$` anchors.
//! The whole text must match.

use crate::types::{PinkDbError, Result};

/// Longest pattern accepted; beyond this the compiled NFA gets too big to be
/// simulated cheaply.
const MAX_PATTERN: usize = 1024;
/// Largest counted repetition accepted (`a{1,1000}`).
const MAX_REPEAT: u32 = 1000;

/// Does `text` match the whole of `pattern`?
pub(crate) fn full_match(pattern: &str, text: &str) -> Result<bool> {
    if pattern.chars().count() > MAX_PATTERN {
        return Err(too_complex());
    }
    let mut parser = Parser {
        src: pattern.chars().collect(),
        pos: 0,
    };
    let node = parser.alternation()?;
    if parser.pos != parser.src.len() {
        // A stray `)` — the caller wrote an unbalanced group.
        return Err(PinkDbError::Parse(format!(
            "invalid SIMILAR TO pattern \"{pattern}\": unbalanced \")\""
        )));
    }
    let mut nfa = Nfa::default();
    let (start, open) = nfa.compile(&node);
    let accept = nfa.push(State::Accept);
    nfa.patch(&open, accept);
    Ok(nfa.is_match(start, &text.chars().collect::<Vec<char>>()))
}

fn too_complex() -> PinkDbError {
    PinkDbError::Parse("SIMILAR TO pattern too complex".to_string())
}

fn bad_pattern(why: &str) -> PinkDbError {
    PinkDbError::Parse(format!("invalid SIMILAR TO pattern: {why}"))
}

#[derive(Debug)]
enum Node {
    Empty,
    Char(char),
    Any,
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
    Start,
    End,
    Concat(Vec<Node>),
    Alt(Vec<Node>),
    Repeat {
        node: Box<Node>,
        min: u32,
        max: Option<u32>,
    },
}

struct Parser {
    src: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.src.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    /// `seq ('|' seq)*`
    fn alternation(&mut self) -> Result<Node> {
        let mut arms = vec![self.sequence()?];
        while self.peek() == Some('|') {
            self.pos += 1;
            arms.push(self.sequence()?);
        }
        Ok(if arms.len() == 1 {
            arms.pop().expect("one arm")
        } else {
            Node::Alt(arms)
        })
    }

    /// `quantified*`, up to `|` or `)`.
    fn sequence(&mut self) -> Result<Node> {
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            let atom = self.atom()?;
            items.push(self.quantify(atom)?);
        }
        Ok(match items.len() {
            0 => Node::Empty,
            1 => items.pop().expect("one item"),
            _ => Node::Concat(items),
        })
    }

    fn atom(&mut self) -> Result<Node> {
        let c = self.bump().expect("caller checked for a char");
        Ok(match c {
            '(' => {
                let inner = self.alternation()?;
                if self.bump() != Some(')') {
                    return Err(bad_pattern("unbalanced \"(\""));
                }
                inner
            }
            '[' => self.class()?,
            '.' => Node::Any,
            '^' => Node::Start,
            '$' => Node::End,
            '\\' => Node::Char(self.bump().ok_or_else(|| bad_pattern("trailing \"\\\""))?),
            '*' | '+' | '?' => {
                return Err(bad_pattern(&format!("\"{c}\" has nothing to repeat")));
            }
            c => Node::Char(c),
        })
    }

    /// `[...]`: ranges, a leading `^` negates, a leading `]` is a literal.
    fn class(&mut self) -> Result<Node> {
        let mut negated = false;
        if self.peek() == Some('^') {
            negated = true;
            self.pos += 1;
        }
        let mut ranges: Vec<(char, char)> = Vec::new();
        let mut first = true;
        loop {
            let c = match self.bump() {
                Some(c) => c,
                None => return Err(bad_pattern("unbalanced \"[\"")),
            };
            if c == ']' && !first {
                break;
            }
            first = false;
            let lo = if c == '\\' {
                self.bump().ok_or_else(|| bad_pattern("trailing \"\\\""))?
            } else {
                c
            };
            // `a-c` is a range; a `-` right before `]` is a literal.
            if self.peek() == Some('-') && self.src.get(self.pos + 1) != Some(&']') {
                self.pos += 1;
                let hi = match self.bump() {
                    Some('\\') => self.bump(),
                    other => other,
                }
                .ok_or_else(|| bad_pattern("unbalanced \"[\""))?;
                ranges.push((lo, hi));
            } else {
                ranges.push((lo, lo));
            }
        }
        Ok(Node::Class { negated, ranges })
    }

    fn quantify(&mut self, atom: Node) -> Result<Node> {
        let (min, max) = match self.peek() {
            Some('*') => {
                self.pos += 1;
                (0, None)
            }
            Some('+') => {
                self.pos += 1;
                (1, None)
            }
            Some('?') => {
                self.pos += 1;
                (0, Some(1))
            }
            Some('{') => {
                // `{n}`, `{n,}`, `{n,m}`; anything else is a literal `{`.
                let save = self.pos;
                self.pos += 1;
                match self.counted() {
                    Some((min, max)) => (min, max),
                    None => {
                        self.pos = save;
                        return Ok(atom);
                    }
                }
            }
            _ => return Ok(atom),
        };
        if min > MAX_REPEAT || max.is_some_and(|m| m > MAX_REPEAT) {
            return Err(too_complex());
        }
        if let Some(max) = max
            && max < min
        {
            return Err(bad_pattern("bad repetition range"));
        }
        Ok(Node::Repeat {
            node: Box::new(atom),
            min,
            max,
        })
    }

    /// The `n`, `n,` or `n,m` inside `{…}`, with the closing `}` consumed.
    fn counted(&mut self) -> Option<(u32, Option<u32>)> {
        let digits = |p: &mut Self| -> Option<u32> {
            let start = p.pos;
            let mut n: u32 = 0;
            while let Some(c) = p.peek() {
                match c.to_digit(10) {
                    Some(d) => {
                        n = n.checked_mul(10)?.checked_add(d)?;
                        p.pos += 1;
                    }
                    None => break,
                }
            }
            (p.pos > start).then_some(n)
        };
        let min = digits(self)?;
        match self.peek() {
            Some('}') => {
                self.pos += 1;
                Some((min, Some(min)))
            }
            Some(',') => {
                self.pos += 1;
                let max = digits(self);
                if self.peek() != Some('}') {
                    return None;
                }
                self.pos += 1;
                Some((min, max))
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
enum State {
    /// Consume `c`, then continue at the second field.
    Char(char, usize),
    Any(usize),
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
        next: usize,
    },
    /// Junction: either branch, no input consumed.
    Split(usize, usize),
    /// Succeed only at the start of the text.
    Start(usize),
    /// Succeed only at the end of the text.
    End(usize),
    /// A patchable edge; `usize::MAX` until [`Nfa::patch`] fills it in.
    Hole(usize),
    Accept,
}

#[derive(Default)]
struct Nfa {
    states: Vec<State>,
}

impl Nfa {
    fn push(&mut self, state: State) -> usize {
        self.states.push(state);
        self.states.len() - 1
    }

    /// Fill every open edge in `open` with a jump to `target`.
    fn patch(&mut self, open: &[usize], target: usize) {
        for &hole in open {
            debug_assert!(matches!(self.states[hole], State::Hole(_)));
            self.states[hole] = State::Hole(target);
        }
    }

    fn hole(&mut self) -> usize {
        self.push(State::Hole(usize::MAX))
    }

    /// Compile `node` into `(entry, open)` — the open edges are the states
    /// whose successor is not known yet (exactly the places a concatenation
    /// continues).
    fn compile(&mut self, node: &Node) -> (usize, Vec<usize>) {
        match node {
            Node::Empty => {
                let h = self.hole();
                (h, vec![h])
            }
            Node::Char(c) => {
                let h = self.hole();
                (self.push(State::Char(*c, h)), vec![h])
            }
            Node::Any => {
                let h = self.hole();
                (self.push(State::Any(h)), vec![h])
            }
            Node::Start => {
                let h = self.hole();
                (self.push(State::Start(h)), vec![h])
            }
            Node::End => {
                let h = self.hole();
                (self.push(State::End(h)), vec![h])
            }
            Node::Class { negated, ranges } => {
                let h = self.hole();
                let class = State::Class {
                    negated: *negated,
                    ranges: ranges.clone(),
                    next: h,
                };
                (self.push(class), vec![h])
            }
            Node::Concat(items) => {
                let mut it = items.iter();
                let (entry, mut open) = self.compile(it.next().expect("concat has items"));
                for item in it {
                    let (next_entry, next_open) = self.compile(item);
                    self.patch(&open, next_entry);
                    open = next_open;
                }
                (entry, open)
            }
            Node::Alt(arms) => {
                let mut it = arms.iter().rev();
                let (mut entry, mut open) = self.compile(it.next().expect("alt has arms"));
                for arm in it {
                    let (arm_entry, arm_open) = self.compile(arm);
                    entry = self.push(State::Split(arm_entry, entry));
                    open.extend(arm_open);
                }
                (entry, open)
            }
            Node::Repeat { node, min, max } => self.repeat(node, *min, *max),
        }
    }

    /// `x{m,n}`: `m` mandatory copies, then optional ones (or a loop for an
    /// unbounded `n`). Each optional copy is a `Split` whose skip branch stays
    /// open, so the caller's continuation patches both outcomes.
    fn repeat(&mut self, node: &Node, min: u32, max: Option<u32>) -> (usize, Vec<usize>) {
        let (entry, mut open) = if min == 0 {
            let h = self.hole();
            (h, vec![h])
        } else {
            self.compile(node)
        };
        for _ in 1..min {
            let (copy_entry, copy_open) = self.compile(node);
            self.patch(&open, copy_entry);
            open = copy_open;
        }
        match max {
            None => {
                let (copy_entry, copy_open) = self.compile(node);
                let skip = self.hole();
                let split = self.push(State::Split(copy_entry, skip));
                self.patch(&open, split);
                // After one iteration, loop back to the same split.
                self.patch(&copy_open, split);
                (entry, vec![skip])
            }
            Some(max) => {
                for _ in min..max {
                    let (copy_entry, copy_open) = self.compile(node);
                    let skip = self.hole();
                    let split = self.push(State::Split(copy_entry, skip));
                    self.patch(&open, split);
                    open = copy_open;
                    open.push(skip);
                }
                (entry, open)
            }
        }
    }

    /// Lock-step simulation: the set of states reachable after each char,
    /// closed over the epsilon edges that hold at that position.
    fn is_match(&self, start: usize, text: &[char]) -> bool {
        let len = text.len();
        let mut current = Vec::new();
        self.close(&mut current, start, 0, len);
        for (i, c) in text.iter().enumerate() {
            let mut next = Vec::new();
            for &s in &current {
                let target = match self.states[s] {
                    State::Char(want, next) if want == *c => Some(next),
                    State::Any(next) => Some(next),
                    State::Class {
                        negated,
                        ref ranges,
                        next,
                    } => {
                        let hit = ranges.iter().any(|(lo, hi)| *lo <= *c && *c <= *hi);
                        (hit != negated).then_some(next)
                    }
                    _ => None,
                };
                if let Some(t) = target {
                    self.close(&mut next, t, i + 1, len);
                }
            }
            current = next;
            if current.is_empty() {
                return false;
            }
        }
        current
            .iter()
            .any(|&s| matches!(self.states[s], State::Accept))
    }

    /// Add `start` and every state reachable without consuming input at `pos`
    /// (of `len` chars) to `set`.
    fn close(&self, set: &mut Vec<usize>, start: usize, pos: usize, len: usize) {
        let mut stack = vec![start];
        while let Some(s) = stack.pop() {
            if set.contains(&s) {
                continue;
            }
            set.push(s);
            match self.states[s] {
                State::Split(a, b) => {
                    stack.push(a);
                    stack.push(b);
                }
                State::Hole(target) => {
                    debug_assert_ne!(target, usize::MAX, "unpatched NFA edge");
                    stack.push(target);
                }
                State::Start(next) if pos == 0 => stack.push(next),
                State::End(next) if pos == len => stack.push(next),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::full_match;

    #[test]
    fn anchors_and_literals() {
        assert!(full_match("abc", "abc").unwrap());
        assert!(!full_match("abc", "abcd").unwrap());
        assert!(!full_match("abc", "xabc").unwrap());
        assert!(full_match("^abc$", "abc").unwrap());
        // `%` and `_` are literals in DuckDB's SIMILAR TO.
        assert!(!full_match("abc%", "abcx").unwrap());
        assert!(full_match("abc%", "abc%").unwrap());
        assert!(!full_match("a_c", "abc").unwrap());
        assert!(full_match("a.c", "abc").unwrap());
        assert!(full_match("", "").unwrap());
        assert!(!full_match("", "x").unwrap());
    }

    #[test]
    fn classes_groups_and_quantifiers() {
        assert!(full_match("[a-c]", "b").unwrap());
        assert!(!full_match("[a-c]", "d").unwrap());
        assert!(full_match("[^a-c]", "d").unwrap());
        assert!(!full_match("[^a-c]", "b").unwrap());
        assert!(full_match("c(at|ow)", "cat").unwrap());
        assert!(full_match("c(at|ow)", "cow").unwrap());
        assert!(!full_match("c(at)", "cow").unwrap());
        assert!(full_match("ab+", "abbb").unwrap());
        assert!(!full_match("ab*c", "acc").unwrap());
        assert!(full_match("ab?c", "ac").unwrap());
        assert!(full_match("a{2}b", "aab").unwrap());
        assert!(!full_match("a{2}", "ac").unwrap());
        assert!(full_match("a{2,}", "aaaa").unwrap());
        assert!(!full_match("a{2,}", "a").unwrap());
        assert!(full_match("a{0,2}b", "b").unwrap());
        assert!(full_match("a{0,2}b", "aab").unwrap());
        assert!(!full_match("a{0,2}b", "aaab").unwrap());
        assert!(full_match("(ab){2}", "abab").unwrap());
        assert!(full_match("(ab)*c", "ababc").unwrap());
        assert!(full_match(".*", "anything at all").unwrap());
        assert!(full_match(r"\.", ".").unwrap());
        assert!(!full_match(r"\.", "a").unwrap());
        assert!(full_match("a-b", "a-b").unwrap());
        assert!(full_match("[]]", "]").unwrap());
    }

    #[test]
    fn refuses_runaway_patterns() {
        assert!(full_match(&"a".repeat(2000), "a").is_err());
        assert!(full_match("a{1,100000}", "a").is_err());
        assert!(full_match("(a", "a").is_err());
        assert!(full_match("a)", "a").is_err());
        assert!(full_match("[a", "a").is_err());
        assert!(full_match("*a", "a").is_err());
    }
}
