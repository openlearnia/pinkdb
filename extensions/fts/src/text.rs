//! The tokenizer: lowercasing, accent stripping, the `ignore` regex and the
//! stopword list.
//!
//! DuckDB's fts tokenizes a field by lowercasing it, optionally stripping
//! accents, then splitting on the **ignored** character runs — every character
//! class that matches `ignore` acts as a separator. Its default
//! `ignore := '(\\.|[^a-z])+'` (DuckDB strings do not escape: a backslash
//! plus the character after it, or any non-letter) therefore keeps letters and
//! drops everything else, including the punctuation of "don't", the digits of
//! "1984" and an escape such as a literal `\n`.
//! This module reproduces that pipeline, in that order, so a token list is a
//! function of the field's text and the index's four text options.

use crate::Stemmer;

/// DuckDB's default `ignore` regex.
pub const DEFAULT_IGNORE: &str = r"(\\.|[^a-z])+";

/// DuckDB's `stopwords := 'english'` list: the SMART system's 571 English
/// stop words. DuckDB ships this list with its fts extension; it is the one
/// `stopwords := 'english'` selects.
pub const ENGLISH_STOPWORDS: &[&str] = &[
    "a",
    "a's",
    "able",
    "about",
    "above",
    "according",
    "accordingly",
    "across",
    "actually",
    "after",
    "afterwards",
    "again",
    "against",
    "ain't",
    "all",
    "allow",
    "allows",
    "almost",
    "alone",
    "along",
    "already",
    "also",
    "although",
    "always",
    "am",
    "among",
    "amongst",
    "an",
    "and",
    "another",
    "any",
    "anybody",
    "anyhow",
    "anyone",
    "anything",
    "anyway",
    "anyways",
    "anywhere",
    "apart",
    "appear",
    "appreciate",
    "appropriate",
    "are",
    "aren't",
    "around",
    "as",
    "aside",
    "ask",
    "asking",
    "associated",
    "at",
    "available",
    "away",
    "awfully",
    "b",
    "be",
    "became",
    "because",
    "become",
    "becomes",
    "becoming",
    "been",
    "before",
    "beforehand",
    "behind",
    "being",
    "believe",
    "below",
    "beside",
    "besides",
    "best",
    "better",
    "between",
    "beyond",
    "both",
    "brief",
    "but",
    "by",
    "c",
    "c'mon",
    "c's",
    "came",
    "can",
    "can't",
    "cannot",
    "cant",
    "cause",
    "causes",
    "certain",
    "certainly",
    "changes",
    "clearly",
    "co",
    "com",
    "come",
    "comes",
    "concerning",
    "consequently",
    "consider",
    "considering",
    "contain",
    "containing",
    "contains",
    "corresponding",
    "could",
    "couldn't",
    "course",
    "currently",
    "d",
    "definitely",
    "described",
    "despite",
    "did",
    "didn't",
    "different",
    "do",
    "does",
    "doesn't",
    "doing",
    "don't",
    "done",
    "down",
    "downwards",
    "during",
    "e",
    "each",
    "edu",
    "eg",
    "eight",
    "either",
    "else",
    "elsewhere",
    "enough",
    "entirely",
    "especially",
    "et",
    "etc",
    "even",
    "ever",
    "every",
    "everybody",
    "everyone",
    "everything",
    "everywhere",
    "ex",
    "exactly",
    "example",
    "except",
    "f",
    "far",
    "few",
    "fifth",
    "first",
    "five",
    "followed",
    "following",
    "follows",
    "for",
    "former",
    "formerly",
    "forth",
    "four",
    "from",
    "further",
    "furthermore",
    "g",
    "get",
    "gets",
    "getting",
    "given",
    "gives",
    "go",
    "goes",
    "going",
    "gone",
    "got",
    "gotten",
    "greetings",
    "h",
    "had",
    "hadn't",
    "happens",
    "hardly",
    "has",
    "hasn't",
    "have",
    "haven't",
    "having",
    "he",
    "he's",
    "hello",
    "help",
    "hence",
    "her",
    "here",
    "here's",
    "hereafter",
    "hereby",
    "herein",
    "hereupon",
    "hers",
    "herself",
    "hi",
    "him",
    "himself",
    "his",
    "hither",
    "hopefully",
    "how",
    "howbeit",
    "however",
    "i",
    "i'd",
    "i'll",
    "i'm",
    "i've",
    "ie",
    "if",
    "ignored",
    "immediate",
    "in",
    "inasmuch",
    "inc",
    "indeed",
    "indicate",
    "indicated",
    "indicates",
    "inner",
    "insofar",
    "instead",
    "into",
    "inward",
    "is",
    "isn't",
    "it",
    "it'd",
    "it'll",
    "it's",
    "its",
    "itself",
    "j",
    "just",
    "k",
    "keep",
    "keeps",
    "kept",
    "know",
    "known",
    "knows",
    "l",
    "last",
    "lately",
    "later",
    "latter",
    "latterly",
    "least",
    "less",
    "lest",
    "let",
    "let's",
    "like",
    "liked",
    "likely",
    "little",
    "look",
    "looking",
    "looks",
    "ltd",
    "m",
    "mainly",
    "many",
    "may",
    "maybe",
    "me",
    "mean",
    "meanwhile",
    "merely",
    "might",
    "more",
    "moreover",
    "most",
    "mostly",
    "much",
    "must",
    "my",
    "myself",
    "n",
    "name",
    "namely",
    "nd",
    "near",
    "nearly",
    "necessary",
    "need",
    "needs",
    "neither",
    "never",
    "nevertheless",
    "new",
    "next",
    "nine",
    "no",
    "nobody",
    "non",
    "none",
    "noone",
    "nor",
    "normally",
    "not",
    "nothing",
    "novel",
    "now",
    "nowhere",
    "o",
    "obviously",
    "of",
    "off",
    "often",
    "oh",
    "ok",
    "okay",
    "old",
    "on",
    "once",
    "one",
    "ones",
    "only",
    "onto",
    "or",
    "other",
    "others",
    "otherwise",
    "ought",
    "our",
    "ours",
    "ourselves",
    "out",
    "outside",
    "over",
    "overall",
    "own",
    "p",
    "particular",
    "particularly",
    "per",
    "perhaps",
    "placed",
    "please",
    "plus",
    "possible",
    "presumably",
    "probably",
    "provides",
    "q",
    "que",
    "quite",
    "qv",
    "r",
    "rather",
    "rd",
    "re",
    "really",
    "reasonably",
    "regarding",
    "regardless",
    "regards",
    "relatively",
    "respectively",
    "right",
    "s",
    "said",
    "same",
    "saw",
    "say",
    "saying",
    "says",
    "second",
    "secondly",
    "see",
    "seeing",
    "seem",
    "seemed",
    "seeming",
    "seems",
    "seen",
    "self",
    "selves",
    "sensible",
    "sent",
    "serious",
    "seriously",
    "seven",
    "several",
    "shall",
    "she",
    "should",
    "shouldn't",
    "since",
    "six",
    "so",
    "some",
    "somebody",
    "somehow",
    "someone",
    "something",
    "sometime",
    "sometimes",
    "somewhat",
    "somewhere",
    "soon",
    "sorry",
    "specified",
    "specify",
    "specifying",
    "still",
    "sub",
    "such",
    "sup",
    "sure",
    "t",
    "t's",
    "take",
    "taken",
    "tell",
    "tends",
    "th",
    "than",
    "thank",
    "thanks",
    "thanx",
    "that",
    "that's",
    "thats",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "thence",
    "there",
    "there's",
    "thereafter",
    "thereby",
    "therefore",
    "therein",
    "thereof",
    "theres",
    "thereupon",
    "these",
    "they",
    "they'd",
    "they'll",
    "they're",
    "they've",
    "think",
    "third",
    "this",
    "thither",
    "thorough",
    "thoroughly",
    "those",
    "though",
    "three",
    "through",
    "throughout",
    "thru",
    "thus",
    "to",
    "together",
    "too",
    "took",
    "toward",
    "towards",
    "tried",
    "tries",
    "truly",
    "try",
    "trying",
    "twice",
    "two",
    "u",
    "un",
    "under",
    "unfortunately",
    "unless",
    "unlikely",
    "until",
    "unto",
    "up",
    "upon",
    "us",
    "use",
    "used",
    "useful",
    "uses",
    "using",
    "usually",
    "v",
    "value",
    "various",
    "very",
    "via",
    "viz",
    "vs",
    "w",
    "want",
    "wants",
    "was",
    "wasn't",
    "way",
    "we",
    "we'd",
    "we'll",
    "we're",
    "we've",
    "welcome",
    "well",
    "went",
    "were",
    "weren't",
    "what",
    "what's",
    "whatever",
    "when",
    "whence",
    "whenever",
    "where",
    "where's",
    "whereafter",
    "whereas",
    "whereby",
    "wherein",
    "whereupon",
    "wherever",
    "whether",
    "which",
    "while",
    "whither",
    "who",
    "who's",
    "whoever",
    "whole",
    "whom",
    "whose",
    "why",
    "will",
    "willing",
    "wish",
    "with",
    "within",
    "without",
    "won't",
    "wonder",
    "would",
    "wouldn't",
    "x",
    "y",
    "yes",
    "yet",
    "you",
    "you'd",
    "you'll",
    "you're",
    "you've",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "z",
    "zero",
];

/// The text options an index was built with.
#[derive(Clone, Debug)]
pub struct TextOptions {
    pub ignore: String,
    pub lower: bool,
    pub strip_accents: bool,
    pub stopwords: Option<&'static [&'static str]>,
}

impl Default for TextOptions {
    fn default() -> Self {
        Self {
            ignore: DEFAULT_IGNORE.to_string(),
            lower: true,
            strip_accents: true,
            stopwords: Some(ENGLISH_STOPWORDS),
        }
    }
}

/// The `ignore` pattern, either DuckDB's default or a compiled custom regex.
#[derive(Clone, Debug)]
enum Ignore {
    /// `(\\.|[^a-z])+`: a run of separators, each either a backslash plus
    /// the character after it (any but a newline, as `.` is) or a character
    /// that is not a lowercase ASCII letter. So `foo\nbar` (a literal
    /// backslash-n) is `foo`, `bar`, not `foo`, `nbar`.
    Default,
    #[cfg(feature = "custom-ignore")]
    Custom(regex::Regex),
}

/// A compiled tokenizer: [`TextOptions`] plus the `ignore` scanner.
#[derive(Clone, Debug)]
pub struct Tokenizer {
    ignore: Ignore,
    lower: bool,
    strip_accents: bool,
    stopwords: Option<&'static [&'static str]>,
}

impl Tokenizer {
    pub fn new(options: &TextOptions) -> Result<Self, String> {
        let ignore = if options.ignore == DEFAULT_IGNORE {
            Ignore::Default
        } else {
            #[cfg(feature = "custom-ignore")]
            {
                Ignore::Custom(regex::Regex::new(&options.ignore).map_err(|e| {
                    format!("ignore regular expression \"{}\": {e}", options.ignore)
                })?)
            }
            #[cfg(not(feature = "custom-ignore"))]
            {
                return Err(format!(
                    "ignore regular expression \"{}\": custom patterns need the \
                     `custom-ignore` feature of pinkdb-fts (rebuild with the \
                     `fts-regex` feature, e.g. --features fts-regex)",
                    options.ignore
                ));
            }
        };
        Ok(Self {
            ignore,
            lower: options.lower,
            strip_accents: options.strip_accents,
            stopwords: options.stopwords,
        })
    }

    /// The field's tokens, in order, before stemming.
    pub fn tokens(&self, text: &str) -> Vec<String> {
        let mut field = text.to_string();
        if self.lower {
            field = field.to_lowercase();
        }
        if self.strip_accents {
            field = strip_accents(&field);
        }
        let mut out = Vec::new();
        for token in self.split(&field) {
            if self
                .stopwords
                .is_some_and(|list| list.contains(&token.as_str()))
            {
                continue;
            }
            out.push(token);
        }
        out
    }

    /// The field's runs of non-separator characters, in order.
    fn split(&self, field: &str) -> Vec<String> {
        match &self.ignore {
            Ignore::Default => {
                let mut tokens = Vec::new();
                let mut token = String::new();
                let mut chars = field.chars().peekable();
                while let Some(c) = chars.next() {
                    if c.is_ascii_lowercase() {
                        token.push(c);
                        continue;
                    }
                    // The `\\.` alternative is tried first: it eats the
                    // escaped character too, even a letter.
                    if c == '\\' && chars.peek().is_some_and(|&n| n != '\n') {
                        chars.next();
                    }
                    if !token.is_empty() {
                        tokens.push(std::mem::take(&mut token));
                    }
                }
                if !token.is_empty() {
                    tokens.push(token);
                }
                tokens
            }
            #[cfg(feature = "custom-ignore")]
            Ignore::Custom(regex) => regex
                .replace_all(field, " ")
                .split_whitespace()
                .map(str::to_string)
                .collect(),
        }
    }

    /// The stemmed tokens an index stores for a field.
    pub fn indexed_tokens(&self, text: &str, stemmer: Stemmer) -> Vec<String> {
        self.tokens(text)
            .into_iter()
            .map(|t| stemmer.stem(&t))
            .collect()
    }
}

/// Remove diacritics the way DuckDB's `strip_accents := 1` does: keep the base
/// letter of a character that decomposes (canonically) into a letter plus
/// combining marks. Letters without such a decomposition — `ß`, `æ`, `ø`,
/// `ł`, `đ` — are not accents and stay.
pub fn strip_accents(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match ascii_base(c) {
            Some(base) => out.push_str(base),
            None => out.push(c),
        }
    }
    out
}

/// The unaccented spelling of `c`, when it has one.
fn ascii_base(c: char) -> Option<&'static str> {
    let base = match c {
        'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' => "A",
        'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' => "C",
        'È' | 'É' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => "E",
        'Ì' | 'Í' | 'Î' | 'Ï' | 'Ĩ' | 'Ī' | 'Ĭ' | 'Į' | 'İ' => "I",
        'Ñ' | 'Ń' | 'Ņ' | 'Ň' => "N",
        'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ō' | 'Ŏ' | 'Ő' => "O",
        'Ù' | 'Ú' | 'Û' | 'Ü' | 'Ũ' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' => "U",
        'Ý' | 'Ŷ' | 'Ÿ' => "Y",
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a",
        'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => "c",
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e",
        'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' | 'ı' => "i",
        'ñ' | 'ń' | 'ņ' | 'ň' | 'ŉ' => "n",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ō' | 'ŏ' | 'ő' => "o",
        'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u",
        'ý' | 'ÿ' | 'ŷ' => "y",
        'Ď' | 'Đ' => "D",
        'ď' | 'đ' => "d",
        'Ĝ' | 'Ğ' | 'Ġ' | 'Ģ' => "G",
        'ĝ' | 'ğ' | 'ġ' | 'ģ' => "g",
        'Ĥ' | 'Ħ' => "H",
        'ĥ' | 'ħ' => "h",
        'Ĵ' => "J",
        'ĵ' => "j",
        'Ķ' => "K",
        'ķ' => "k",
        'Ĺ' | 'Ļ' | 'Ľ' => "L",
        'ĺ' | 'ļ' | 'ľ' => "l",
        'Ŕ' | 'Ŗ' | 'Ř' => "R",
        'ŕ' | 'ŗ' | 'ř' => "r",
        'Ś' | 'Ŝ' | 'Ş' | 'Š' => "S",
        'ś' | 'ŝ' | 'ş' | 'š' => "s",
        'Ţ' | 'Ť' => "T",
        'ţ' | 'ť' => "t",
        'Ŵ' => "W",
        'ŵ' => "w",
        'Ź' | 'Ż' | 'Ž' => "Z",
        'ź' | 'ż' | 'ž' => "z",
        _ => return None,
    };
    Some(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenizer() -> Tokenizer {
        Tokenizer::new(&TextOptions::default()).expect("the default ignore regex compiles")
    }

    #[test]
    fn default_pipeline_keeps_letters_and_drops_the_rest() {
        let t = tokenizer();
        assert_eq!(
            t.tokens("The quick, brown fox — jumps over 2 lazy dogs!"),
            vec!["quick", "brown", "fox", "jumps", "lazy", "dogs"]
        );
        // Punctuation inside a word splits it; digits are separators too.
        // The SMART list's contraction entries (`don't`) can never match:
        // the apostrophe splits the token before the lookup, as in DuckDB.
        // `t` is itself a stopword, so "don't" leaves `don`.
        assert_eq!(t.tokens("don't stop 1984"), vec!["don", "stop"]);
        // Lowercasing happens before the regex, so uppercase words survive.
        assert_eq!(t.tokens("QUICK World"), vec!["quick", "world"]);
        assert_eq!(t.tokens(""), Vec::<String>::new());
        assert_eq!(t.tokens("!!! ..."), Vec::<String>::new());
    }

    #[test]
    fn accented_letters_fold_to_their_base() {
        let t = tokenizer();
        assert_eq!(t.tokens("Café naïve Über"), vec!["cafe", "naive", "uber"]);
        // `ß` is not an accent, so it survives `strip_accents` — and then the
        // default `ignore` regex (ASCII `[a-z]`) splits the word, exactly as
        // DuckDB's does; `e` is a stopword. A Unicode-aware `ignore` keeps it.
        assert_eq!(t.tokens("straße"), vec!["stra"]);
    }

    #[cfg(feature = "custom-ignore")]
    #[test]
    fn lower_and_strip_accents_are_optional() {
        let options = TextOptions {
            lower: false,
            strip_accents: false,
            ignore: r"[^\p{L}]+".to_string(),
            ..TextOptions::default()
        };
        let t = Tokenizer::new(&options).expect("replaceable ignore");
        assert_eq!(t.tokens("Café Noir"), vec!["Café", "Noir"]);
    }

    #[test]
    fn stopwords_are_dropped_after_lowercasing() {
        let t = tokenizer();
        assert_eq!(t.tokens("The cat is on the mat"), vec!["cat", "mat"]);
        let plain = TextOptions {
            stopwords: None,
            ..TextOptions::default()
        };
        assert_eq!(
            Tokenizer::new(&plain)
                .expect("no stopwords")
                .tokens("The cat is on the mat"),
            vec!["the", "cat", "is", "on", "the", "mat"]
        );
    }

    #[cfg(feature = "custom-ignore")]
    #[test]
    fn a_custom_ignore_regex_is_honoured_and_validated() {
        let options = TextOptions {
            ignore: r"[\s,]+".to_string(),
            ..TextOptions::default()
        };
        let t = Tokenizer::new(&options).expect("whitespace-only separators");
        assert_eq!(t.tokens("a-b, c.d"), vec!["a-b", "c.d"]);
        let bad = TextOptions {
            ignore: "(".to_string(),
            ..Default::default()
        };
        let err = Tokenizer::new(&bad).unwrap_err();
        assert!(err.contains("ignore regular expression"), "{err}");
    }

    /// A build without `custom-ignore` refuses a custom pattern, naming the
    /// feature that provides it. The default pattern never needs it.
    #[cfg(not(feature = "custom-ignore"))]
    #[test]
    fn a_custom_ignore_regex_names_the_rebuild_feature() {
        let options = TextOptions {
            ignore: r"[\s,]+".to_string(),
            ..TextOptions::default()
        };
        let err = Tokenizer::new(&options).unwrap_err();
        assert!(err.contains("fts-regex"), "{err}");
        assert!(Tokenizer::new(&TextOptions::default()).is_ok());
    }

    /// The default scanner must separate exactly where DuckDB's default
    /// `ignore := '(\\.|[^a-z])+'` does, on arbitrary input.
    #[test]
    fn the_default_scanner_equals_the_default_regex() {
        let regex = regex::Regex::new(DEFAULT_IGNORE).unwrap();
        // Raw splitting only: no lowercasing, accent stripping or stopwords.
        let t = Tokenizer::new(&TextOptions {
            lower: false,
            strip_accents: false,
            stopwords: None,
            ..TextOptions::default()
        })
        .unwrap();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..2000 {
            let mut text = String::new();
            for _ in 0..16 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                // Backslashes and newlines often, so `\\.` is exercised next
                // to letters, newlines and other separators.
                let c = match state % 8 {
                    0 => '\\',
                    1 => '\n',
                    _ => char::from_u32((state >> 3) as u32 % 0x250).unwrap_or('x'),
                };
                text.push(c);
            }
            let expected: Vec<String> = regex
                .replace_all(&text, " ")
                .split_whitespace()
                .map(str::to_string)
                .collect();
            assert_eq!(t.split(&text), expected, "{text:?}");
        }
    }

    #[test]
    fn a_backslash_escape_is_one_separator_with_its_character() {
        let t = Tokenizer::new(&TextOptions {
            lower: false,
            strip_accents: false,
            stopwords: None,
            ..TextOptions::default()
        })
        .unwrap();
        assert_eq!(t.split("foo\\nbar"), vec!["foo", "bar"]);
        assert_eq!(t.split("a\\\\b"), vec!["a", "b"]);
        assert_eq!(t.split("x\\\nyz"), vec!["x", "yz"]);
        assert_eq!(t.split("end\\"), vec!["end"]);
    }

    #[test]
    fn indexed_tokens_are_stemmed_and_stopwords_go_first() {
        let t = tokenizer();
        assert_eq!(
            t.indexed_tokens("The relational databases", Stemmer::Porter),
            vec!["relat", "databas"]
        );
        assert_eq!(
            t.indexed_tokens("A relational", Stemmer::None),
            vec!["relational"]
        );
    }
}
