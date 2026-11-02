//! CSV import with schema inference and DuckDB's CSV option surface.
//!
//! One [`CsvOptions`] drives both `.import` and `read_csv`, so the same bytes
//! read the same way through either entry point: delimiter/quote/escape
//! dialect, text encoding, header handling, NULL rules and the error policy.
//!
//! Inference rule (per column, over all non-NULL cells): all i32-parseable →
//! INTEGER, all i64 → BIGINT, all f64 → DOUBLE, all true/false
//! (case-insensitive) → BOOLEAN, all dates → DATE, all timestamps →
//! TIMESTAMP, else VARCHAR. An all-empty column → VARCHAR.
//!
//! Import runs a fast path first: infer from a head sample
//! ([`INFER_SAMPLE_ROWS`] rows, like DuckDB), then one parse pass — on the
//! rayon pool for files with a "clean" byte dialect (the default comma/`"`
//! dialect, no `\r`, UTF-8), sequentially through the record reader otherwise.
//! Whenever the sample can't prove the final schema — a later row violates the
//! sampled type, or a column is empty across the sample while rows continue —
//! the import is retried with exact whole-file inference (two passes): same
//! result, just slower. See [`load_bytes`].
//!
//! # Cell rules
//!
//! An unquoted cell is trimmed; a quoted cell keeps its bytes (DuckDB's rule).
//! A cell is NULL when it is empty and the column is not `force_not_null`,
//! or when it equals one of the `nullstr` tokens. A quoted cell is also NULL
//! by that rule unless `allow_quoted_nulls=false`; then quoting is what keeps
//! an empty (or token) cell as an empty string.
//!
//! # Encodings
//!
//! UTF-8 is the default and the only encoding decoded strictly: a cell that is
//! not valid UTF-8 fails with "CSV contains invalid UTF-8". `latin-1` and
//! `windows-1252` map single bytes; `utf-16` LE/BE decodes code units,
//! replacing an unpaired surrogate with U+FFFD. `auto` (the default) strips a
//! UTF-8 BOM and switches to UTF-16 LE/BE when the file starts with that
//! byte-order mark; explicit LE/BE also honor a BOM of either endianness. One
//! leading BOM is always stripped. Decoding is streaming: the record reader
//! pulls UTF-8 through a fixed-size window, so wave 98c's remote read-ahead
//! keeps working.
//!
//! # Local vs remote
//!
//! A local file is sniffed whole. A remote object is sniffed over the
//! read-ahead window (wave 98c's rule) and a larger object is then streamed
//! through the same parser; options never change that split.

use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use rayon::prelude::*;

use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value, parse_date, parse_timestamp};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector};

/// Rows inspected for head-sample inference (DuckDB's documented default).
const INFER_SAMPLE_ROWS: usize = 20_480;
/// Byte blocks scanned per rayon task when locating newlines.
const SCAN_BLOCK: usize = 1 << 20;
/// Lines per batch in the byte fast path (≈ one output chunk).
const BATCH_LINES: usize = CHUNK_SIZE;
/// Raw bytes one decoder fill reads.
const READ_CHUNK: usize = 1 << 16;

/// Rejects a report lists before it is truncated (the total stays exact).
pub const REJECTS_SHOWN: usize = 10;

// ---------------------------------------------------------------------------
// Options.
// ---------------------------------------------------------------------------

/// Text encoding of a CSV file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// UTF-8, or UTF-16 when the file starts with a UTF-16 byte-order mark.
    #[default]
    Auto,
    Utf8,
    Latin1,
    Windows1252,
    Utf16Le,
    Utf16Be,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Auto => "auto",
            Encoding::Utf8 => "utf-8",
            Encoding::Latin1 => "latin-1",
            Encoding::Windows1252 => "windows-1252",
            Encoding::Utf16Le => "utf-16le",
            Encoding::Utf16Be => "utf-16be",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Encoding::Auto),
            "utf-8" | "utf8" => Some(Encoding::Utf8),
            "latin-1" | "latin1" | "iso-8859-1" | "iso8859-1" => Some(Encoding::Latin1),
            "windows-1252" | "win-1252" | "cp1252" => Some(Encoding::Windows1252),
            "utf-16" | "utf16" | "utf-16le" | "utf16le" | "utf-16-le" => Some(Encoding::Utf16Le),
            "utf-16be" | "utf16be" | "utf-16-be" => Some(Encoding::Utf16Be),
            _ => None,
        }
    }
}

/// Whether the first row names the columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Header {
    /// Guess with DuckDB's rule (see [`sniff_csv`]).
    #[default]
    Auto,
    /// The first row is a header (the default once detected).
    Present,
    /// Every row is data; columns are named `column0`, `column1`, ….
    Absent,
}

/// What to do with a row that cannot be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorPolicy {
    /// Fail the import/scan on the first bad row (the default).
    #[default]
    Error,
    /// Skip bad rows and collect a rejects report.
    Warn,
}

/// CSV dialect, encoding, header and NULL rules — the single option set shared
/// by `.import` and `read_csv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOptions {
    /// Field separator (one byte, default `,`).
    pub delimiter: u8,
    /// Quote byte (default `"`).
    pub quote: u8,
    /// Escape byte inside a quoted field (`None` = `""` quoting only).
    pub escape: Option<u8>,
    pub encoding: Encoding,
    pub header: Header,
    pub errors: ErrorPolicy,
    /// Column names, overriding the header row (and its presence is still
    /// decided by `header`).
    pub names: Option<Vec<String>>,
    /// `types` / `dtypes` / `column_types`: override the inferred type of a
    /// column — by name (the `{name: TYPE}` form) or, in the list form, by
    /// position of the list entry.
    pub column_types: Vec<(Option<String>, DataType)>,
    /// `columns`: every column's name and type, read without inference.
    pub columns: Option<(Vec<String>, Vec<DataType>)>,
    /// Cells that read as NULL in addition to the empty-cell rule.
    pub nullstr: Vec<String>,
    /// Keep a quoted empty cell as `""` instead of NULL (DuckDB's
    /// `allow_quoted_nulls`, default `true` = quoted NULLs are NULL).
    pub allow_quoted_nulls: bool,
    /// Columns whose empty cells are empty strings, not NULL.
    pub force_not_null: Vec<String>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: None,
            encoding: Encoding::Auto,
            header: Header::Auto,
            errors: ErrorPolicy::Error,
            names: None,
            column_types: Vec::new(),
            columns: None,
            nullstr: Vec::new(),
            allow_quoted_nulls: true,
            force_not_null: Vec::new(),
        }
    }
}

impl CsvOptions {
    /// Apply one `name=value` option, the shape both `.import` and `read_csv`
    /// accept (names are DuckDB's where DuckDB has one).
    pub fn apply(&mut self, name: &str, value: &str) -> Result<()> {
        let name = name.trim().to_ascii_lowercase();
        // A delimiter may *be* whitespace (`delimiter='\t'`, `' '`), so byte
        // options read the raw value; everything else is trimmed.
        let raw = value;
        let value = value.trim();
        match name.as_str() {
            "delimiter" | "delim" | "sep" | "separator" => self.delimiter = one_byte(&name, raw)?,
            "quote" => self.quote = one_byte(&name, raw)?,
            "escape" => {
                self.escape = if raw.is_empty() {
                    None
                } else {
                    Some(one_byte(&name, raw)?)
                }
            }
            "encoding" => {
                self.encoding = Encoding::parse(value).ok_or_else(|| {
                    PinkDbError::Parameter(format!(
                        "unknown encoding `{value}`: expected auto, utf-8, latin-1, \
                         windows-1252, utf-16le or utf-16be"
                    ))
                })?
            }
            "header" => {
                self.header = match value.to_ascii_lowercase().as_str() {
                    "auto" => Header::Auto,
                    "true" | "1" | "yes" | "on" => Header::Present,
                    "false" | "0" | "no" | "off" => Header::Absent,
                    _ => {
                        return Err(PinkDbError::Parameter(format!(
                            "option `header` expects auto, true or false, got `{value}`"
                        )));
                    }
                }
            }
            "errors" => {
                self.errors = match value.to_ascii_lowercase().as_str() {
                    "error" | "strict" | "fail" => ErrorPolicy::Error,
                    "warn" | "skip" | "ignore" => ErrorPolicy::Warn,
                    _ => {
                        return Err(PinkDbError::Parameter(format!(
                            "option `errors` expects error or warn, got `{value}`"
                        )));
                    }
                }
            }
            "ignore_errors" => {
                self.errors = if parse_bool(&name, value)? {
                    ErrorPolicy::Warn
                } else {
                    ErrorPolicy::Error
                }
            }
            "allow_quoted_nulls" => self.allow_quoted_nulls = parse_bool(&name, value)?,
            "nullstr" | "null" => self.nullstr = split_list(value),
            "names" | "column_names" => self.names = Some(split_list(value)),
            "force_not_null" => self.force_not_null = split_list(value),
            // `types`/`dtypes`/`column_types` override the inferred type of the
            // named columns; `columns` gives every name and type, so nothing is
            // inferred.
            "types" | "dtypes" | "column_types" => {
                self.column_types = parse_type_overrides(&name, value)?
            }
            "columns" => self.columns = Some(parse_columns(value)?),
            // Recognised, still unimplemented: refused precisely, never ignored.
            "auto_detect" | "all_varchar" | "sample_size" | "skip" | "store_rejects"
            | "rejects_table" | "rejects_scan" | "rejects_limit" | "strict_mode"
            | "null_padding" | "compression" | "new_line" | "comment" | "date_format"
            | "dateformat" | "timestamp_format" | "timestampformat" | "decimal_separator"
            | "thousands" | "normalize_names" | "union_by_name" | "filename"
            | "hive_partitioning" | "file_row_number" | "parallel" | "buffer_size"
            | "max_line_size" | "maximum_line_size" => {
                return Err(PinkDbError::Parameter(format!(
                    "CSV option `{name}` is not supported yet"
                )));
            }
            other => {
                return Err(PinkDbError::Parameter(format!(
                    "unknown CSV option `{other}`"
                )));
            }
        }
        Ok(())
    }
}

/// One single-byte option value, accepting `\t`/`\n`/`\r` spellings for the
/// control characters a shell cannot type easily.
fn one_byte(name: &str, value: &str) -> Result<u8> {
    let byte = match value {
        "\\t" => b'\t',
        "\\n" => b'\n',
        "\\r" => b'\r',
        _ if value.len() == 1 => value.as_bytes()[0],
        _ => {
            return Err(PinkDbError::Parameter(format!(
                "option `{name}` takes exactly one byte, got `{value}`"
            )));
        }
    };
    Ok(byte)
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(PinkDbError::Parameter(format!(
            "option `{name}` expects true or false, got `{value}`"
        ))),
    }
}

/// A comma-separated option list (`nullstr=NA,N/A`, `names=a,b`). The scalar
/// parameter shape has no list literal, so a comma is the separator; entries
/// are trimmed and empty ones dropped.
fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// The body of a `{…}` struct literal, if `text` is one.
fn struct_body(text: &str) -> Option<&str> {
    text.trim().strip_prefix('{')?.strip_suffix('}')
}

/// The body of a `[…]` list literal, if `text` is one.
fn list_body(text: &str) -> Option<&str> {
    text.trim().strip_prefix('[')?.strip_suffix(']')
}

/// Split a literal at its top-level commas: a quoted name or the parentheses of
/// a parameterized type (`DECIMAL(10,2)`) are not separators.
fn split_top_level(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut quote = None;
    let mut start = 0;
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                continue;
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    out.push(&text[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    out.push(&text[start..]);
    out.into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect()
}

/// A name or type as written in a literal: `DAY_ID` or `'DAY_ID'`.
fn unquote(text: &str) -> &str {
    let text = text.trim();
    for q in ['\'', '"'] {
        if let Some(inner) = text.strip_prefix(q).and_then(|r| r.strip_suffix(q)) {
            return inner;
        }
    }
    text
}

/// A type name out of a CSV option: `DATE`, `DECIMAL(10,2)`.
fn parse_type_name(text: &str) -> Result<DataType> {
    let text = unquote(text);
    let unknown = || PinkDbError::Type(format!("unknown CSV column type `{text}`"));
    let (name, params) = match text.split_once('(') {
        Some((name, rest)) => {
            let inner = rest.strip_suffix(')').ok_or_else(unknown)?;
            let params: Option<Vec<i64>> = inner
                .split(',')
                .map(|p| p.trim().parse::<i64>().ok())
                .collect();
            (name.trim(), params.ok_or_else(unknown)?)
        }
        None => (text, Vec::new()),
    };
    DataType::from_sql_params(name, &params).ok_or_else(unknown)
}

/// `types`/`dtypes`/`column_types`: `{name: TYPE, …}` names the columns, a
/// `[TYPE, …]` list gives them by position.
fn parse_type_overrides(name: &str, text: &str) -> Result<Vec<(Option<String>, DataType)>> {
    if let Some(body) = struct_body(text) {
        return split_top_level(body)
            .into_iter()
            .map(|entry| {
                let (column, dtype) = entry.split_once(':').ok_or_else(|| {
                    PinkDbError::Parameter(format!(
                        "option `{name}` entry `{entry}` is not `name: TYPE`"
                    ))
                })?;
                Ok((Some(unquote(column).to_string()), parse_type_name(dtype)?))
            })
            .collect();
    }
    if let Some(body) = list_body(text) {
        return split_top_level(body)
            .into_iter()
            .map(|dtype| Ok((None, parse_type_name(dtype)?)))
            .collect();
    }
    Err(PinkDbError::Parameter(format!(
        "option `{name}` expects `{{name: TYPE, …}}` or `[TYPE, …]`, got `{text}`"
    )))
}

/// `columns`: `{name: TYPE, …}` for every column.
fn parse_columns(text: &str) -> Result<(Vec<String>, Vec<DataType>)> {
    let body = struct_body(text).ok_or_else(|| {
        PinkDbError::Catalog("read_csv columns requires a struct as input".to_string())
    })?;
    let mut names = Vec::new();
    let mut types = Vec::new();
    for entry in split_top_level(body) {
        let (column, dtype) = entry.split_once(':').ok_or_else(|| {
            PinkDbError::Parameter(format!(
                "option `columns` entry `{entry}` is not `name: TYPE`"
            ))
        })?;
        names.push(unquote(column).to_string());
        types.push(parse_type_name(dtype)?);
    }
    Ok((names, types))
}

// ---------------------------------------------------------------------------
// Streaming text decoder: any supported encoding → UTF-8 bytes.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Utf8,
    Latin1,
    Windows1252,
    Utf16Le,
    Utf16Be,
}

/// A `Read` adapter decoding `input` to UTF-8 on the fly. Fixed memory: one
/// raw chunk plus one decoded chunk, and at most three carried bytes (a split
/// UTF-16 surrogate pair).
struct Decoder<R> {
    input: R,
    requested: Encoding,
    mode: Option<Mode>,
    raw: Vec<u8>,
    out: Vec<u8>,
    out_pos: usize,
    eof: bool,
    chunk: Vec<u8>,
}

impl<R: Read> Decoder<R> {
    fn new(input: R, requested: Encoding) -> Self {
        Self {
            input,
            requested,
            mode: None,
            raw: Vec::new(),
            out: Vec::new(),
            out_pos: 0,
            eof: false,
            chunk: vec![0; READ_CHUNK],
        }
    }

    fn read_raw(&mut self) -> Result<()> {
        let n = self.input.read(&mut self.chunk).map_err(PinkDbError::Io)?;
        if n == 0 {
            self.eof = true;
        } else {
            self.raw.extend_from_slice(&self.chunk[..n]);
        }
        Ok(())
    }

    /// Decode the next chunk. `false` means end of input.
    fn fill(&mut self) -> Result<bool> {
        self.out.clear();
        self.out_pos = 0;
        while self.out.is_empty() && !self.eof {
            self.read_raw()?;
            if self.mode.is_none() {
                self.mode = Some(self.detect()?);
            }
            decode(self.mode.unwrap(), &mut self.raw, &mut self.out, self.eof);
        }
        Ok(!self.out.is_empty())
    }

    /// Pick the decoding mode and strip one byte-order mark.
    fn detect(&mut self) -> Result<Mode> {
        // A BOM is at most three bytes; top up so a short first read cannot
        // hide it.
        while self.raw.len() < 3 && !self.eof {
            self.read_raw()?;
        }
        let raw = &mut self.raw;
        let utf8_bom = raw.starts_with(&[0xEF, 0xBB, 0xBF]);
        let le_bom = raw.starts_with(&[0xFF, 0xFE]);
        let be_bom = raw.starts_with(&[0xFE, 0xFF]);
        let mode = match self.requested {
            Encoding::Auto if le_bom => Mode::Utf16Le,
            Encoding::Auto if be_bom => Mode::Utf16Be,
            Encoding::Auto => Mode::Utf8,
            Encoding::Utf8 => Mode::Utf8,
            Encoding::Latin1 => Mode::Latin1,
            Encoding::Windows1252 => Mode::Windows1252,
            // An explicit UTF-16 request still honors a BOM of either
            // endianness: `encoding='utf-16'` must read both.
            Encoding::Utf16Le if be_bom => Mode::Utf16Be,
            Encoding::Utf16Be if le_bom => Mode::Utf16Le,
            Encoding::Utf16Le => Mode::Utf16Le,
            Encoding::Utf16Be => Mode::Utf16Be,
        };
        let bom_len = match mode {
            Mode::Utf8 if utf8_bom => 3,
            Mode::Utf16Le | Mode::Utf16Be if le_bom || be_bom => 2,
            _ => 0,
        };
        raw.drain(..bom_len);
        Ok(mode)
    }
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.out_pos == self.out.len() {
            if !self.fill().map_err(std::io::Error::other)? {
                return Ok(0);
            }
        }
        let n = buf.len().min(self.out.len() - self.out_pos);
        buf[..n].copy_from_slice(&self.out[self.out_pos..self.out_pos + n]);
        self.out_pos += n;
        Ok(n)
    }
}

/// Decode complete code points from `raw` into UTF-8 `out`, keeping an
/// incomplete tail when the stream continues.
fn decode(mode: Mode, raw: &mut Vec<u8>, out: &mut Vec<u8>, eof: bool) {
    match mode {
        Mode::Utf8 => {
            out.extend_from_slice(raw);
            raw.clear();
        }
        Mode::Latin1 => {
            for &b in raw.iter() {
                push_code_point(out, b as u32);
            }
            raw.clear();
        }
        Mode::Windows1252 => {
            for &b in raw.iter() {
                push_code_point(out, cp1252(b) as u32);
            }
            raw.clear();
        }
        Mode::Utf16Le | Mode::Utf16Be => {
            let little = mode == Mode::Utf16Le;
            let unit = |i: usize| {
                if little {
                    u16::from_le_bytes([raw[i], raw[i + 1]])
                } else {
                    u16::from_be_bytes([raw[i], raw[i + 1]])
                }
            };
            let n = raw.len();
            let mut i = 0;
            while i + 1 < n {
                let hi = unit(i);
                if (0xD800..0xDC00).contains(&hi) {
                    if i + 3 >= n {
                        if !eof {
                            break; // the low surrogate is in the next chunk
                        }
                        push_code_point(out, 0xFFFD);
                        i += 2;
                        continue;
                    }
                    let lo = unit(i + 2);
                    if (0xDC00..0xE000).contains(&lo) {
                        let cp = 0x10000 + ((hi as u32 - 0xD800) << 10) + (lo as u32 - 0xDC00);
                        push_code_point(out, cp);
                    } else {
                        push_code_point(out, 0xFFFD);
                    }
                    i += 4;
                    continue;
                }
                push_code_point(
                    out,
                    if (0xDC00..0xE000).contains(&hi) {
                        0xFFFD
                    } else {
                        hi as u32
                    },
                );
                i += 2;
            }
            raw.drain(..i);
        }
    }
}

fn push_code_point(out: &mut Vec<u8>, code: u32) {
    let c = char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER);
    let mut buf = [0u8; 4];
    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
}

/// Windows-1252's 0x80–0x9F block; the five unassigned bytes keep their C1
/// code point (what the byte would mean in latin-1).
fn cp1252(b: u8) -> u16 {
    const HIGH: [u16; 32] = [
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160,
        0x2039, 0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022,
        0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
    ];
    match b {
        0x80..=0x9F => HIGH[(b - 0x80) as usize],
        _ => b as u16,
    }
}

// ---------------------------------------------------------------------------
// Record reader: the dialect, one record at a time, over a byte window.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Cell {
    start: usize,
    end: usize,
    /// The field started with a quote (so it keeps its bytes verbatim).
    quoted: bool,
}

/// One parsed record: concatenated field bytes plus their ranges.
#[derive(Debug, Default)]
struct Record {
    buf: Vec<u8>,
    cells: Vec<Cell>,
    /// Source line of the record's first byte (1-based).
    line: u64,
}

impl Record {
    fn new() -> Self {
        Self::default()
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.cells.clear();
    }

    fn text(&self, i: usize) -> Result<&str> {
        let cell = self.cells[i];
        std::str::from_utf8(&self.buf[cell.start..cell.end])
            .map_err(|_| PinkDbError::Execution("CSV contains invalid UTF-8".to_string()))
    }

    fn cell_text(&self, cell: Cell) -> Result<&str> {
        std::str::from_utf8(&self.buf[cell.start..cell.end])
            .map_err(|_| PinkDbError::Execution("CSV contains invalid UTF-8".to_string()))
    }
}

/// The record reader: decodes `input` and yields logical records with the
/// configured dialect, skipping blank lines (DuckDB's rule too).
struct Records<R> {
    input: Decoder<R>,
    win: Vec<u8>,
    pos: usize,
    eof: bool,
    chunk: Vec<u8>,
    line: u64,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
}

impl<R: Read> Records<R> {
    fn new(input: R, opts: &CsvOptions) -> Self {
        Self {
            input: Decoder::new(input, opts.encoding),
            win: Vec::new(),
            pos: 0,
            eof: false,
            chunk: vec![0; READ_CHUNK],
            line: 1,
            delimiter: opts.delimiter,
            quote: opts.quote,
            escape: opts.escape,
        }
    }

    /// The byte at `i`, reading more input when the window ends there.
    fn at(&mut self, i: usize) -> Result<Option<u8>> {
        while i >= self.win.len() {
            if self.eof {
                return Ok(None);
            }
            let n = self.input.read(&mut self.chunk).map_err(PinkDbError::Io)?;
            if n == 0 {
                self.eof = true;
            } else {
                self.win.extend_from_slice(&self.chunk[..n]);
            }
        }
        Ok(Some(self.win[i]))
    }

    fn next(&mut self, rec: &mut Record) -> Result<bool> {
        loop {
            rec.clear();
            if !self.parse_one(rec)? {
                return Ok(false);
            }
            // A blank line (one empty unquoted field) is not a record.
            if !(rec.cells.len() == 1
                && !rec.cells[0].quoted
                && rec.cells[0].start == rec.cells[0].end)
            {
                return Ok(true);
            }
        }
    }

    fn parse_one(&mut self, rec: &mut Record) -> Result<bool> {
        if self.pos > 0 {
            self.win.drain(..self.pos);
            self.pos = 0;
        }
        rec.line = self.line;
        let mut i = 0usize;
        let mut field_start = 0usize;
        let mut quoted = false;
        let mut in_quotes = false;
        let mut started = false;
        loop {
            let Some(b) = self.at(i)? else {
                if !started {
                    return Ok(false);
                }
                rec.cells.push(Cell {
                    start: field_start,
                    end: rec.buf.len(),
                    quoted,
                });
                self.pos = i;
                return Ok(true);
            };
            started = true;
            if in_quotes {
                if b == self.quote {
                    if self.at(i + 1)? == Some(self.quote) {
                        rec.buf.push(self.quote);
                        i += 2;
                    } else {
                        in_quotes = false;
                        i += 1;
                    }
                } else if self.escape == Some(b) {
                    if let Some(escaped) = self.at(i + 1)? {
                        rec.buf.push(escaped);
                        i += 2;
                    } else {
                        i += 1; // a dangling escape at EOF: dropped
                    }
                } else {
                    if b == b'\n' {
                        self.line += 1;
                    }
                    rec.buf.push(b);
                    i += 1;
                }
            } else if b == self.delimiter {
                rec.cells.push(Cell {
                    start: field_start,
                    end: rec.buf.len(),
                    quoted,
                });
                field_start = rec.buf.len();
                quoted = false;
                i += 1;
            } else if b == b'\n' || b == b'\r' {
                if b == b'\r' && self.at(i + 1)? == Some(b'\n') {
                    i += 2;
                } else {
                    i += 1;
                }
                rec.cells.push(Cell {
                    start: field_start,
                    end: rec.buf.len(),
                    quoted,
                });
                self.line += 1;
                self.pos = i;
                return Ok(true);
            } else if b == self.quote && rec.buf.len() == field_start {
                quoted = true;
                in_quotes = true;
                i += 1;
            } else {
                rec.buf.push(b);
                i += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inference.
// ---------------------------------------------------------------------------

/// Streaming inference state: folded per cell, no row storage.
/// All-non-empty-cells-agree, computed in one pass.
#[derive(Clone)]
struct InferState {
    non_empty: usize,
    is_bool: bool,
    is_i32: bool,
    is_i64: bool,
    is_f64: bool,
    is_date: bool,
    is_ts: bool,
}

impl InferState {
    fn new() -> Self {
        Self {
            non_empty: 0,
            is_bool: true,
            is_i32: true,
            is_i64: true,
            is_f64: true,
            is_date: true,
            is_ts: true,
        }
    }

    fn feed(&mut self, cell: &str) {
        let c = trim_cell(cell);
        if c.is_empty() {
            return;
        }
        self.non_empty += 1;
        if !(self.is_bool
            || self.is_i32
            || self.is_i64
            || self.is_f64
            || self.is_date
            || self.is_ts)
        {
            return; // Every flag is dead: the column is VARCHAR already.
        }
        // Case-insensitive compare without allocating (hot path: every cell).
        if self.is_bool && !(c.eq_ignore_ascii_case("true") || c.eq_ignore_ascii_case("false")) {
            self.is_bool = false;
        }
        // An i32 parse implies i64 and f64; an i64 parse implies f64, so the
        // wider parses are skipped while the narrower flag holds.
        if self.is_i32 && c.parse::<i32>().is_ok() {
            // i32 ⇒ i64 ⇒ f64 (flags stay true).
        } else {
            self.is_i32 = false;
            if self.is_i64 && c.parse::<i64>().is_ok() {
                // i64 ⇒ f64 (flag stays true).
            } else {
                self.is_i64 = false;
                if self.is_f64 && c.parse::<f64>().is_err() {
                    self.is_f64 = false;
                }
            }
        }
        if self.is_date && parse_date(c).is_none() {
            self.is_date = false;
        }
        if self.is_ts && parse_timestamp(c).is_none() {
            self.is_ts = false;
        }
    }

    /// A cell kept as an empty string (`force_not_null`): no type parses it.
    fn feed_kept_empty(&mut self) {
        self.non_empty += 1;
        self.is_bool = false;
        self.is_i32 = false;
        self.is_i64 = false;
        self.is_f64 = false;
        self.is_date = false;
        self.is_ts = false;
    }

    fn dtype(&self) -> DataType {
        if self.non_empty == 0 {
            DataType::Varchar
        } else if self.is_bool {
            DataType::Boolean
        } else if self.is_i32 {
            DataType::Integer
        } else if self.is_i64 {
            DataType::Bigint
        } else if self.is_f64 {
            DataType::Double
        } else if self.is_date {
            DataType::Date
        } else if self.is_ts {
            DataType::Timestamp
        } else {
            DataType::Varchar
        }
    }
}

/// Infer one column's type from its non-empty cells.
fn infer_column(cells: &[&str]) -> DataType {
    let mut st = InferState::new();
    for c in cells {
        st.feed(c);
    }
    st.dtype()
}

/// Infer a full schema from header + string records.
pub fn infer_schema(headers: &[String], records: &[Vec<String>]) -> Vec<DataType> {
    (0..headers.len())
        .map(|col| {
            let cells: Vec<&str> = records
                .iter()
                .map(|row| row.get(col).map(String::as_str).unwrap_or(""))
                .collect();
            infer_column(&cells)
        })
        .collect()
}

/// A sniffed CSV: the columns a read produces and the options that produced
/// them (with `header` resolved — `Auto` never survives a sniff).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvSchema {
    pub names: Vec<String>,
    pub types: Vec<DataType>,
    pub options: CsvOptions,
    /// The sample stopped before the file did and a column had no value in it,
    /// so the whole file may hold a type the sample never saw: `.import`
    /// falls back to the exact pass rather than trusting `types`.
    pub partial: bool,
}

/// Sniff a CSV: column names, header decision and inferred types from the
/// first [`INFER_SAMPLE_ROWS`] rows — DuckDB's `sample_size` default, and the
/// cost that keeps a large import one parse pass. This is what `.import`
/// creates and what the `read_csv` table source binds, so the two agree by
/// construction; a later row that breaks a sampled type is a `Type` error
/// (`.import` retries over the whole file, a scan reports it).
///
/// `header=auto` follows DuckDB's rule: the first row is a header when the
/// inferred types change without it, or when the remaining rows are all
/// VARCHAR (case probes: `a,b\nc,d` → header, `1,2\n3,4` → no header). A file
/// with no data rows keeps the row as a header when it is all VARCHAR.
pub fn sniff_csv(data: &[u8], opts: &CsvOptions) -> Result<CsvSchema> {
    sniff_csv_limited(data, opts, INFER_SAMPLE_ROWS)
}

/// Whole-file inference: the `.import` retry path, where the types must cover
/// every row.
fn sniff_csv_exact(data: &[u8], opts: &CsvOptions) -> Result<CsvSchema> {
    sniff_csv_limited(data, opts, usize::MAX)
}

fn sniff_csv_limited(data: &[u8], opts: &CsvOptions, limit: usize) -> Result<CsvSchema> {
    let first = sniff(data, opts, &[], limit)?;
    let forced = forced_indices(&first, opts)?;
    if forced.is_empty() {
        Ok(first)
    } else {
        sniff(data, opts, &forced, limit)
    }
}

/// Column indices named by `force_not_null` (case-insensitive).
fn forced_indices(schema: &CsvSchema, opts: &CsvOptions) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for name in &opts.force_not_null {
        match schema
            .names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(name))
        {
            Some(i) => out.push(i),
            None => {
                return Err(PinkDbError::Catalog(format!(
                    "force_not_null names unknown column \"{name}\""
                )));
            }
        }
    }
    Ok(out)
}

fn sniff(data: &[u8], opts: &CsvOptions, forced: &[usize], limit: usize) -> Result<CsvSchema> {
    // `columns` names and types every column, so nothing is inferred and the
    // first row is the header (unless `header=false`).
    if let Some((names, types)) = &opts.columns {
        let mut options = opts.clone();
        options.header = match options.header {
            Header::Auto => Header::Present,
            header => header,
        };
        return Ok(CsvSchema {
            names: names.clone(),
            types: types.clone(),
            options,
            partial: false,
        });
    }
    let mut records = Records::new(std::io::Cursor::new(data), opts);
    let mut rec = Record::new();
    if !records.next(&mut rec)? {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    let width = opts.names.as_ref().map_or(rec.cells.len(), Vec::len);
    if width == 0 {
        return Err(PinkDbError::Catalog("CSV has no header row".to_string()));
    }
    let mut first_names = Vec::with_capacity(width);
    for i in 0..width {
        let name = if i < rec.cells.len() {
            rec.text(i)?
        } else {
            ""
        };
        first_names.push(if name.is_empty() {
            format!("column{i}")
        } else {
            name.to_string()
        });
    }
    let mut all = vec![InferState::new(); width];
    let mut skip = vec![InferState::new(); width];
    let mut data_rows = 0usize;
    match opts.header {
        Header::Present => {}
        Header::Absent => {
            feed_record(&mut all, &rec, opts, forced)?;
            data_rows += 1;
        }
        // The first row may be the header: count it in `all` only.
        Header::Auto => feed_record(&mut all, &rec, opts, forced)?,
    }
    while data_rows < limit && records.next(&mut rec)? {
        feed_record(&mut all, &rec, opts, forced)?;
        if opts.header == Header::Auto {
            feed_record(&mut skip, &rec, opts, forced)?;
        }
        data_rows += 1;
    }
    // Stopping at the sample only matters when rows remain: a file that fits
    // in the sample has been inferred exactly.
    let truncated = data_rows >= limit && records.next(&mut rec)?;
    let types_all = dtypes(&all);
    let types_skip = dtypes(&skip);
    let header = match opts.header {
        Header::Present => true,
        Header::Absent => false,
        Header::Auto if data_rows == 0 => types_all.iter().all(|t| *t == DataType::Varchar),
        Header::Auto => {
            types_all != types_skip || types_skip.iter().all(|t| *t == DataType::Varchar)
        }
    };
    let names = match &opts.names {
        Some(names) => names.clone(),
        None if header => first_names,
        None => (0..width).map(|i| format!("column{i}")).collect(),
    };
    let types = if opts.header == Header::Auto && header {
        types_skip
    } else {
        types_all
    };
    // `types={'DAY_ID': 'DATE'}`: the named (or positional) columns take the
    // given type back from inference.
    let types = if opts.column_types.is_empty() {
        types
    } else {
        let mut types = types;
        let width = types.len();
        for (pos, (name, dtype)) in opts.column_types.iter().enumerate() {
            let idx = match name {
                Some(name) => names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        PinkDbError::Catalog(format!(
                            "COLUMN_TYPES error: Columns with names: \"{name}\" do not exist \
                             in the CSV File"
                        ))
                    })?,
                None => pos,
            };
            if idx >= width {
                return Err(PinkDbError::Catalog(format!(
                    "COLUMN_TYPES error: {} types given for {width} columns",
                    opts.column_types.len()
                )));
            }
            types[idx] = *dtype;
        }
        types
    };
    let mut options = opts.clone();
    options.header = if header {
        Header::Present
    } else {
        Header::Absent
    };
    // A column with no value in the sample could still be anything: the exact
    // pass must look at the whole file.
    let partial = truncated
        && if header {
            skip.iter().any(|st| st.non_empty == 0)
        } else {
            all.iter().any(|st| st.non_empty == 0)
        };
    Ok(CsvSchema {
        names,
        types,
        options,
        partial,
    })
}

fn dtypes(states: &[InferState]) -> Vec<DataType> {
    states.iter().map(InferState::dtype).collect()
}

/// Fold one record into the inference states. The candidate header row is fed
/// like data: DuckDB decides by comparing the two inferences.
fn feed_record(
    states: &mut [InferState],
    rec: &Record,
    opts: &CsvOptions,
    forced: &[usize],
) -> Result<()> {
    for (i, st) in states.iter_mut().enumerate() {
        let Some(cell) = rec.cells.get(i).copied() else {
            continue;
        };
        let raw = rec.cell_text(cell)?;
        let text = if cell.quoted { raw } else { trim_cell(raw) };
        if is_null_text(text, cell.quoted, opts, forced.contains(&i)) {
            continue;
        }
        if text.is_empty() {
            st.feed_kept_empty();
        } else {
            st.feed(text);
        }
    }
    Ok(())
}

/// The cell rules' NULL test (see the module docs).
fn is_null_text(text: &str, quoted: bool, opts: &CsvOptions, force_not_null: bool) -> bool {
    if quoted && !opts.allow_quoted_nulls {
        return false;
    }
    (text.is_empty() && !force_not_null) || opts.nullstr.iter().any(|n| n == text)
}

// ---------------------------------------------------------------------------
// Values.
// ---------------------------------------------------------------------------

/// `str::trim` with a fast path: most CSV cells carry no surrounding
/// whitespace, so the Unicode-aware scan is skipped unless a boundary byte
/// could be whitespace (ASCII whitespace or non-ASCII).
#[inline]
fn trim_cell(cell: &str) -> &str {
    const fn is_ascii_ws(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
    }
    let clean = match (cell.as_bytes().first(), cell.as_bytes().last()) {
        (Some(&f), Some(&l)) => f.is_ascii() && l.is_ascii() && !is_ascii_ws(f) && !is_ascii_ws(l),
        _ => true, // empty: nothing to trim
    };
    if clean { cell } else { cell.trim() }
}

/// Parse one non-NULL cell as `dtype`.
fn field_value(text: &str, dtype: DataType) -> Result<Value> {
    // A quoted cell keeps its surrounding whitespace (DuckDB does the same), so
    // VARCHAR sees the raw text; every other parser needs a bare literal.
    let text = match dtype {
        DataType::Varchar => text,
        _ => trim_cell(text),
    };
    Ok(match dtype {
        DataType::Boolean => {
            if text.eq_ignore_ascii_case("true") {
                Value::Boolean(true)
            } else if text.eq_ignore_ascii_case("false") {
                Value::Boolean(false)
            } else {
                return Err(PinkDbError::Type(format!(
                    "cannot parse \"{text}\" as BOOLEAN"
                )));
            }
        }
        DataType::Uuid => Value::Uuid(
            crate::uuid::parse(text)
                .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{text}\" as UUID")))?,
        ),
        DataType::Decimal { precision, scale } => {
            let v = crate::types::parse_decimal_str(text, precision, scale)
                .map_err(PinkDbError::Type)?;
            Value::decimal(v, scale)
        }
        DataType::Interval => match crate::types::parse_interval(text) {
            Some(iv) => Value::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            },
            None => {
                return Err(PinkDbError::Type(format!(
                    "cannot parse \"{text}\" as INTERVAL"
                )));
            }
        },
        DataType::Integer => text
            .parse::<i32>()
            .map(Value::Integer)
            .map_err(|_| PinkDbError::Type(format!("cannot parse \"{text}\" as INTEGER")))?,
        DataType::Bigint => text
            .parse::<i64>()
            .map(Value::Bigint)
            .map_err(|_| PinkDbError::Type(format!("cannot parse \"{text}\" as BIGINT")))?,
        // The CSV reader has no 128-bit integer path yet (step 5 of the
        // HUGEINT plan); the sniffer never infers it, but a declared column
        // can name it.
        DataType::Hugeint | DataType::Uhugeint => {
            return Err(PinkDbError::Execution(format!(
                "{} is not yet supported in CSV",
                dtype.name()
            )));
        }
        DataType::Double => text
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|_| PinkDbError::Type(format!("cannot parse \"{text}\" as DOUBLE")))?,
        DataType::Varchar => Value::Varchar(text.to_string()),
        DataType::Date => Value::Date(
            parse_date(text)
                .ok_or_else(|| PinkDbError::Type(format!("cannot parse \"{text}\" as DATE")))?,
        ),
        DataType::Timestamp => {
            Value::Timestamp(parse_timestamp(text).ok_or_else(|| {
                PinkDbError::Type(format!("cannot parse \"{text}\" as TIMESTAMP"))
            })?)
        }
        // A CSV cell for an ARRAY column is the `[a, b]` text form (what
        // CAST and SELECT print), so a CSV export/import round-trips.
        DataType::Array { elem, len } => Value::Array(Arc::new(
            crate::array::ArrayValue::parse(text, elem, len)
                .map_err(|e| PinkDbError::Type(format!("cannot parse \"{text}\" as ARRAY: {e}")))?,
        )),
        // A CSV cell for a VARIANT column is JSON text (what CAST and SELECT
        // print), so the round trip through a CSV export/import holds.
        DataType::Variant => {
            Value::Variant(Arc::new(crate::variant::from_json_str(text).map_err(
                |e| PinkDbError::Type(format!("cannot parse \"{text}\" as VARIANT: {e}")),
            )?))
        }
    })
}

/// Parse one raw CSV field into `col` (empty → NULL).
fn push_field(col: &mut Vector, field: &str, dtype: DataType) -> Result<()> {
    let c = trim_cell(field);
    if c.is_empty() {
        return col.push_null();
    }
    col.push_valid(field_value(c, dtype)?)
}

/// Default table name = file stem (e.g. `sales.csv` → `sales`; a URL's
/// query/fragment is ignored, so `https://h/x.csv?token=…` → `x`).
fn default_table_name(path: &str) -> String {
    let lower = path.to_ascii_lowercase();
    let path = if lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("s3://")
    {
        path.split(['?', '#']).next().unwrap_or(path)
    } else {
        path
    };
    Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("imported")
        .to_string()
}

// ---------------------------------------------------------------------------
// URL imports (wave 75): a host-installed whole-object fetcher.
//
// The engine cannot depend on the transport crate (`pinkdb-httpfs` depends on
// the engine for its `read_csv` table source), so `.import https://…` and
// `import_csv(url)` reach the transport through this process-global hook. The
// CLI's `httpfs` feature installs `pinkdb_httpfs::UrlFetcher` at startup; a
// build without it refuses a URL with a clear feature error, never treating
// the URL as a local path.

/// A host-installed reader for URL imports: `.import https://…/x.csv`,
/// `.import s3://bucket/x.csv`, and [`import_csv`].
///
/// The hook is process-level like extensions and table sources: not
/// persisted, gone after a reopen, and it must be registered before a durable
/// import is replayed. `fetch` returns already-human-readable errors (the
/// engine wraps them in [`PinkDbError::Execution`]).
pub trait UrlFetcher: std::fmt::Debug + Send + Sync {
    /// Whether this fetcher serves `url` (normally a scheme match).
    fn supports(&self, url: &str) -> bool;

    /// The whole object's bytes.
    fn fetch(&self, url: &str) -> std::result::Result<Vec<u8>, String>;
}

/// Register a URL fetcher process-wide (idempotent by identity: the newest
/// registration is consulted first; multiple fetchers may coexist by scheme).
pub fn register_global_url_fetcher(fetcher: Arc<dyn UrlFetcher>) {
    let mut fetchers = global_url_fetchers()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    fetchers.insert(0, fetcher);
}

fn global_url_fetchers() -> &'static Mutex<Vec<Arc<dyn UrlFetcher>>> {
    static GLOBAL: OnceLock<Mutex<Vec<Arc<dyn UrlFetcher>>>> = OnceLock::new();
    GLOBAL.get_or_init(|| Mutex::new(Vec::new()))
}

/// Whether `path` is an import URL (any `scheme://`).
fn is_import_url(path: &str) -> bool {
    let Some((scheme, _rest)) = path.split_once("://") else {
        return false;
    };
    !scheme.is_empty()
        && scheme
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
}

fn read_import_bytes(path: &str) -> Result<Vec<u8>> {
    if is_import_url(path) {
        let fetcher = global_url_fetchers()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .find(|f| f.supports(path))
            .cloned();
        let Some(fetcher) = fetcher else {
            return Err(PinkDbError::Execution(format!(
                "cannot import \"{path}\": no URL fetcher is registered in this build; \
                 build with the `httpfs` feature (or register one with \
                 `pinkdb::csv::register_global_url_fetcher`)"
            )));
        };
        return fetcher
            .fetch(path)
            .map_err(|e| PinkDbError::Execution(format!("cannot import \"{path}\": {e}")));
    }
    Ok(std::fs::read(path)?)
}

// ---------------------------------------------------------------------------
// Rejects (the `errors=warn` report).
// ---------------------------------------------------------------------------

/// One skipped row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    pub line: u64,
    /// The column that failed, when the row itself was well formed.
    pub column: Option<String>,
    pub reason: String,
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.column {
            Some(column) => write!(
                f,
                "line {}, column \"{column}\": {}",
                self.line, self.reason
            ),
            None => write!(f, "line {}: {}", self.line, self.reason),
        }
    }
}

/// The rejects of one import: the first [`REJECTS_SHOWN`] with their details,
/// plus the exact total.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rejects {
    pub total: u64,
    pub shown: Vec<Reject>,
}

impl Rejects {
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Fold another batch's rejects in (same import, second pass).
    fn append(&mut self, other: &mut Rejects) {
        self.total += other.total;
        for reject in other.shown.drain(..) {
            if self.shown.len() < REJECTS_SHOWN {
                self.shown.push(reject);
            }
        }
    }

    fn push(&mut self, line: u64, column: Option<&str>, reason: String) {
        self.total += 1;
        if self.shown.len() < REJECTS_SHOWN {
            self.shown.push(Reject {
                line,
                column: column.map(str::to_string),
                reason,
            });
        }
    }
}

impl std::fmt::Display for Rejects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.total == 1 {
            writeln!(f, "1 row rejected:")?;
        } else {
            writeln!(f, "{} rows rejected:", self.total)?;
        }
        for reject in &self.shown {
            writeln!(f, "  {reject}")?;
        }
        if self.total > self.shown.len() as u64 {
            writeln!(f, "  … {} more", self.total - self.shown.len() as u64)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parsing rows into chunks.
// ---------------------------------------------------------------------------

/// Fresh per-column builders for one output chunk.
fn new_chunk(types: &[DataType]) -> Vec<Vector> {
    types
        .iter()
        .map(|t| Vector::with_capacity(*t, CHUNK_SIZE))
        .collect()
}

/// Which file field feeds one output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// A constant NULL (a column the file does not have).
    Null,
    /// File field `idx`; `keep_empty` keeps its empty cells as `""`.
    Field { idx: usize, keep_empty: bool },
}

fn identity_sources(schema: &CsvSchema) -> Vec<Source> {
    let forced = schema
        .options
        .force_not_null
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect::<Vec<_>>();
    (0..schema.names.len())
        .map(|idx| Source::Field {
            idx,
            keep_empty: forced.contains(&schema.names[idx].to_ascii_lowercase()),
        })
        .collect()
}

/// Read every record, mapping file fields into `types`-shaped rows and
/// emitting each [`CHUNK_SIZE`]-row batch. `sources` names, per output
/// column, which file field (or NULL) feeds it; `skip_header` drops the first
/// record (the caller may have consumed it already) and `line_base` is the
/// number of records before `input`'s first byte.
///
/// A row with more fields than the header is a structural error; a cell that
/// does not parse as the column's type is a row error. Either fails the read
/// under `errors=error` and skips the row under `errors=warn`.
fn read_rows<R: Read>(
    input: R,
    schema: &CsvSchema,
    types: &[DataType],
    sources: &[Source],
    skip_header: bool,
    line_base: u64,
    emit: &mut dyn FnMut(Vec<Vector>) -> Result<()>,
) -> Result<(usize, Rejects)> {
    let opts = &schema.options;
    let width = schema.names.len();
    let mut records = Records::new(input, opts);
    let mut rec = Record::new();
    let mut skip_header = skip_header;
    let mut chunk = new_chunk(types);
    let mut in_chunk = 0usize;
    let mut rows = 0usize;
    let mut rejects = Rejects::default();
    let mut row: Vec<Value> = Vec::with_capacity(types.len());
    while records.next(&mut rec)? {
        if skip_header {
            skip_header = false;
            continue;
        }
        let line = rec.line + line_base;
        if rec.cells.len() > width {
            let reason = format!(
                "row has {} fields but the header has {width}",
                rec.cells.len()
            );
            if opts.errors == ErrorPolicy::Error {
                return Err(PinkDbError::Csv(Box::new(crate::types::CsvError {
                    message: reason,
                    line: Some(line),
                    column: None,
                })));
            }
            rejects.push(line, None, reason);
            continue;
        }
        row.clear();
        let mut bad: Option<(usize, PinkDbError)> = None;
        for (col, source) in sources.iter().enumerate() {
            let value = match *source {
                Source::Null => Ok(Value::Null),
                Source::Field { idx, keep_empty } => match rec.cells.get(idx).copied() {
                    None => Ok(Value::Null),
                    Some(cell) => {
                        let raw = rec.cell_text(cell)?;
                        let text = if cell.quoted { raw } else { trim_cell(raw) };
                        if is_null_text(text, cell.quoted, opts, keep_empty) {
                            Ok(Value::Null)
                        } else {
                            field_value(text, types[col])
                        }
                    }
                },
            };
            match value {
                Ok(v) => row.push(v),
                Err(e) => {
                    bad = Some((col, e));
                    break;
                }
            }
        }
        if let Some((col, e)) = bad {
            if opts.errors == ErrorPolicy::Error {
                return Err(e);
            }
            let name = match sources[col] {
                Source::Field { idx, .. } => schema.names.get(idx).map(String::as_str),
                Source::Null => schema.names.get(col).map(String::as_str),
            };
            rejects.push(line, name, e.to_string());
            continue;
        }
        for (col, value) in row.drain(..).enumerate() {
            chunk[col].push_value(value)?;
        }
        rows += 1;
        in_chunk += 1;
        if in_chunk == CHUNK_SIZE {
            emit(std::mem::replace(&mut chunk, new_chunk(types)))?;
            in_chunk = 0;
        }
    }
    if in_chunk > 0 {
        emit(chunk)?;
    }
    Ok((rows, rejects))
}

/// Parse CSV bytes into engine chunks, one chunk per [`CHUNK_SIZE`] rows.
pub fn parse_csv_chunks(data: &[u8], schema: &CsvSchema) -> Result<Vec<DataChunk>> {
    let mut chunks = Vec::new();
    // The whole buffer is in hand, and the batch parser proves the dialect
    // before it emits anything, so a refusal costs nothing.
    let ends = if fast_eligible(data, &schema.options) && data.len() <= u32::MAX as usize {
        record_ends(data, false)
    } else {
        None
    };
    if let Some(newlines) = ends {
        clean_batches(
            data,
            &newlines,
            &schema.types,
            schema.options.header == Header::Present,
            0,
            &mut |columns| {
                chunks.push(DataChunk::new(columns)?);
                Ok(())
            },
        )?;
        return Ok(chunks);
    }
    let sources = identity_sources(schema);
    read_rows(
        std::io::Cursor::new(data),
        schema,
        &schema.types,
        &sources,
        skip_header(schema),
        0,
        &mut |chunk| {
            chunks.push(DataChunk::new(chunk)?);
            Ok(())
        },
    )?;
    Ok(chunks)
}

/// Parse CSV rows arriving from `input` (a streamed remote or large local
/// file), handing each [`CHUNK_SIZE`]-row chunk to `emit` as it is parsed —
/// the bounded-memory form of [`parse_csv_chunks`] (wave 98c). Returns the row
/// count. The batch parser does the work while it can prove the dialect; the
/// record reader finishes whatever it hands back.
pub fn stream_csv_chunks<R: Read>(
    input: R,
    schema: &CsvSchema,
    emit: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<usize> {
    let (rows, lines, rest) = clean_stream(input, schema, emit)?;
    let Some(carry) = rest else {
        return Ok(rows);
    };
    let sources = identity_sources(schema);
    let (more, _rejects) = read_rows(
        std::io::Cursor::new(carry.bytes).chain(carry.input),
        schema,
        &schema.types,
        &sources,
        carry.skip_header,
        lines,
        &mut |columns| emit(DataChunk::new(columns)?),
    )?;
    Ok(rows + more)
}

fn skip_header(schema: &CsvSchema) -> bool {
    schema.options.header == Header::Present
}

/// Bytes the batch parser buffers from a stream before it hands the rest to
/// the record reader.
const STREAM_BLOCK: usize = 8 << 20;

/// Cap on one record that does not fit a window: past this the reader takes
/// over, so the batch parser's memory stays bounded.
const STREAM_CARRY_MAX: usize = 8 * STREAM_BLOCK;

/// What the batch parser could not finish: the bytes it read but did not
/// parse, the rest of the stream, and whether the reader still has to drop a
/// header record (the batch pass may have consumed it already).
struct Carry<R> {
    bytes: Vec<u8>,
    input: R,
    skip_header: bool,
}

/// Parse as much of `input` as the batch parser can prove, one
/// [`STREAM_BLOCK`] window at a time, and hand back what it could not.
/// Returns the rows emitted and the records consumed (the reader's line
/// base). Memory stays bounded by the window plus one record.
fn clean_stream<R: Read>(
    mut input: R,
    schema: &CsvSchema,
    emit: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<(usize, u64, Option<Carry<R>>)> {
    let mut buf: Vec<u8> = Vec::new();
    let mut rows = 0usize;
    let mut lines = 0u64;
    let mut skip = skip_header(schema);
    // The option set is stream-independent except for a byte-order mark, so a
    // dialect the batches cannot model is refused before reading anything.
    if !fast_eligible(&[], &schema.options) {
        return Ok((
            0,
            0,
            Some(Carry {
                bytes: buf,
                input,
                skip_header: skip,
            }),
        ));
    }
    loop {
        let want = buf.len() + STREAM_BLOCK;
        let eof = fill(&mut input, &mut buf, want)?;
        if !fast_eligible(&buf[..buf.len().min(3)], &schema.options) {
            return Ok((
                0,
                0,
                Some(Carry {
                    bytes: buf,
                    input,
                    skip_header: skip,
                }),
            ));
        }
        if eof {
            if buf.is_empty() {
                return Ok((rows, lines, None));
            }
            // The last record has no newline: parse it when the dialect is
            // provable, otherwise the reader takes the tail.
            return match record_ends(&buf, false) {
                Some(ends) => {
                    rows +=
                        clean_batches(&buf, &ends, &schema.types, skip, lines, &mut |columns| {
                            emit(DataChunk::new(columns)?)
                        })?;
                    Ok((rows, lines, None))
                }
                None => Ok((
                    rows,
                    lines,
                    Some(Carry {
                        bytes: buf,
                        input,
                        skip_header: skip,
                    }),
                )),
            };
        }
        // Cut at the last record end the window proves; the tail is one
        // incomplete record, re-read with the next window.
        let Some(ends) = record_ends(&buf, true) else {
            return Ok((
                rows,
                lines,
                Some(Carry {
                    bytes: buf,
                    input,
                    skip_header: skip,
                }),
            ));
        };
        let Some(&last) = ends.last() else {
            // One record spans the windows. Grow, but not without bound: a
            // record this big is the reader's problem.
            if buf.len() >= STREAM_CARRY_MAX {
                return Ok((
                    rows,
                    lines,
                    Some(Carry {
                        bytes: buf,
                        input,
                        skip_header: skip,
                    }),
                ));
            }
            continue;
        };
        let cut = last as usize + 1;
        rows += clean_batches(
            &buf[..cut],
            &ends,
            &schema.types,
            skip,
            lines,
            &mut |columns| emit(DataChunk::new(columns)?),
        )?;
        skip = false;
        lines += ends.len() as u64;
        buf.drain(..cut);
    }
}

/// Read until `buf` holds at least `want` bytes; `true` at end of stream.
fn fill<R: Read>(input: &mut R, buf: &mut Vec<u8>, want: usize) -> Result<bool> {
    while buf.len() < want {
        let start = buf.len();
        buf.resize(want, 0);
        match input.read(&mut buf[start..]) {
            Ok(0) => {
                buf.truncate(start);
                return Ok(true);
            }
            Ok(n) => buf.truncate(start + n),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => buf.truncate(start),
            Err(e) => return Err(PinkDbError::Io(e)),
        }
    }
    Ok(false)
}

/// Parse `data`'s records in [`BATCH_LINES`]-line batches on the rayon pool,
/// handing each chunk to `emit` in file order. `newlines` must be
/// [`record_ends`]' answer for `data`; `skip_header` drops the first record
/// and `line_base` is the number of records before `data` (for error lines).
/// The first failure in file order wins, like the sequential reader.
fn clean_batches(
    data: &[u8],
    newlines: &[u32],
    types: &[DataType],
    skip_header: bool,
    line_base: u64,
    emit: &mut dyn FnMut(Vec<Vector>) -> Result<()>,
) -> Result<usize> {
    // Cut the data region into line batches (line counts, not row counts:
    // empty lines are skipped later, chunk sizes may vary a little).
    let (mut start, mut j) = if skip_header {
        match newlines.first() {
            Some(&end) => (end as usize + 1, BATCH_LINES),
            None => return Ok(0), // header only, no rows
        }
    } else {
        (0, BATCH_LINES - 1)
    };
    let mut batches: Vec<(usize, usize)> = Vec::new();
    while j < newlines.len() {
        let end = newlines[j] as usize + 1;
        if end > start {
            batches.push((start, end));
            start = end;
        }
        j += BATCH_LINES;
    }
    if start < data.len() {
        batches.push((start, data.len()));
    }
    let parsed: Vec<std::result::Result<Vec<Vector>, BatchError>> = batches
        .par_iter()
        .map(|&(s, e)| parse_batch(data, s, e, types))
        .collect();
    let mut rows = 0usize;
    for chunk in parsed {
        let chunk = chunk.map_err(|e| batch_error(e, newlines, types.len(), line_base))?;
        rows += chunk.first().map(|c| c.len()).unwrap_or(0);
        emit(chunk)?;
    }
    Ok(rows)
}

/// The error the sequential reader would have raised for a failed batch.
fn batch_error(err: BatchError, newlines: &[u32], width: usize, line_base: u64) -> PinkDbError {
    match err {
        BatchError::Fields { count, offset } => {
            PinkDbError::Csv(Box::new(crate::types::CsvError {
                message: format!("row has {count} fields but the header has {width}"),
                // The line is only needed on the error path: count the file's
                // newlines before the row instead of tracking it in the loop.
                line: Some(
                    line_base + newlines.partition_point(|&n| (n as usize) < offset) as u64 + 1,
                ),
                column: None,
            }))
        }
        BatchError::Db(e) => e,
    }
}

// ---------------------------------------------------------------------------
// The clean-file fast path (rayon over newline batches).
// ---------------------------------------------------------------------------

/// Failure inside one fast-path batch.
#[derive(Debug)]
enum BatchError {
    /// A row has more fields than the header (structural — never retried).
    /// `offset` is the row's byte offset in the file, for the error's line.
    Fields { count: usize, offset: usize },
    /// A field failed to parse as the sampled type (retries whole-file
    /// inference) or the data is not valid UTF-8 (fatal).
    Db(PinkDbError),
}

/// Outcome of the clean-file fast path.
#[derive(Debug)]
enum CleanError {
    /// The file has features the byte splitter doesn't model (quoting, `\r`,
    /// or >4 GiB): the caller must use the record reader.
    NotClean,
    Db(PinkDbError),
}

/// Parse one line's fields into `chunk`. Empty lines are skipped (the record
/// reader's dialect). Missing fields read as NULL; extra fields error.
fn push_line(
    chunk: &mut [Vector],
    line: &[u8],
    types: &[DataType],
    offset: usize,
) -> std::result::Result<(), BatchError> {
    if line.is_empty() {
        return Ok(());
    }
    let mut fields = Fields::new(line);
    let mut seen = 0usize;
    for (col, dtype) in chunk.iter_mut().zip(types) {
        let (raw, quoted) = fields.next().unwrap_or((&[], false));
        seen += 1;
        let s = std::str::from_utf8(raw).map_err(|_| {
            BatchError::Db(PinkDbError::Execution(
                "CSV contains invalid UTF-8".to_string(),
            ))
        })?;
        if quoted {
            // A quoted empty is NULL here because `fast_eligible` keeps
            // `allow_quoted_nulls := false` off the batch path.
            if s.is_empty() {
                col.push_null().map_err(BatchError::Db)?;
            } else {
                col.push_valid(field_value(s, *dtype).map_err(BatchError::Db)?)
                    .map_err(BatchError::Db)?;
            }
        } else {
            push_field(col, s, *dtype).map_err(BatchError::Db)?;
        }
    }
    if fields.next().is_some() {
        let mut count = seen + 1;
        while fields.next().is_some() {
            count += 1;
        }
        return Err(BatchError::Fields { count, offset });
    }
    Ok(())
}

/// One record's fields, RFC 4180 quoting included: a quoted field keeps its
/// bytes verbatim (`""` becomes `"`, nothing is trimmed), an unquoted field
/// is `push_field`'s business. Only files [`record_ends`] accepted come here,
/// so the quoting scan cannot fail.
struct Fields<'a> {
    line: &'a [u8],
    /// One past the delimiter that ended the field last returned;
    /// `line.len() + 1` once the last field is out.
    pos: usize,
    /// Holds a quoted field that needed `""` unescaping.
    unescaped: Vec<u8>,
}

impl<'a> Fields<'a> {
    fn new(line: &'a [u8]) -> Self {
        Self {
            line,
            pos: 0,
            unescaped: Vec::new(),
        }
    }

    /// The next field, and whether it was quoted.
    fn next(&mut self) -> Option<(&[u8], bool)> {
        if self.pos > self.line.len() {
            return None;
        }
        if self.line.get(self.pos) == Some(&b'"') {
            // Find the closing quote; a doubled quote is content.
            let mut i = self.pos + 1;
            loop {
                let q = i + self.line[i..].iter().position(|&b| b == b'"')?;
                if q + 1 < self.line.len() && self.line[q + 1] == b'"' {
                    i = q + 2;
                    continue;
                }
                let content = &self.line[self.pos + 1..q];
                self.unescaped.clear();
                let mut at = 0;
                while let Some(off) = content[at..].iter().position(|&b| b == b'"') {
                    self.unescaped.extend_from_slice(&content[at..=at + off]);
                    at += off + 2; // the second quote of the pair
                }
                self.unescaped.extend_from_slice(&content[at..]);
                self.pos = q + 1;
                if self.line.get(self.pos) == Some(&b',') || self.pos == self.line.len() {
                    self.pos += 1;
                }
                return Some((&self.unescaped, true));
            }
        }
        let rest = self.line.get(self.pos..)?;
        let end = rest
            .iter()
            .position(|&b| b == b',')
            .map(|off| self.pos + off)
            .unwrap_or(self.line.len());
        let field = &self.line[self.pos..end];
        self.pos = end + 1;
        Some((field, false))
    }
}

/// Parse `data[start..end]` (whole lines) into one chunk of vectors.
fn parse_batch(
    data: &[u8],
    start: usize,
    end: usize,
    types: &[DataType],
) -> std::result::Result<Vec<Vector>, BatchError> {
    let mut chunk = new_chunk(types);
    let mut line_start = start;
    let mut in_quotes = false;
    for i in start..end {
        match data[i] {
            b'"' => in_quotes = !in_quotes,
            b'\n' if !in_quotes => {
                push_line(&mut chunk, &data[line_start..i], types, line_start)?;
                line_start = i + 1;
            }
            _ => {}
        }
    }
    if line_start < end {
        // Final line without a trailing newline (last batch of the file).
        push_line(&mut chunk, &data[line_start..end], types, line_start)?;
    }
    Ok(chunk)
}

/// Offsets of the `\n` that ends each record, or `None` when the batch parser
/// would read the file differently from the record reader and has to hand it
/// over: a `\r` anywhere (the batches keep their line ends), a quote that
/// does not open a field, a quote that does not close before a delimiter or a
/// line end, or (unless `partial`) a quote that never closes. `partial` means
/// `data` is a window of a longer stream whose last record may be incomplete.
/// Every other file is RFC 4180 quoting, which `parse_batch`/`Fields` decode
/// exactly like the record reader. Two parallel passes over ~[`SCAN_BLOCK`]
/// tasks: newline and quote positions, then the quote state each block starts
/// in (an escaped `""` counts twice, so the parity of a block's quotes is its
/// net state change).
fn record_ends(data: &[u8], partial: bool) -> Option<Vec<u32>> {
    let scanned: Vec<Option<(Vec<u32>, Vec<u32>)>> = data
        .par_chunks(SCAN_BLOCK)
        .enumerate()
        .map(|(blk, bytes)| {
            let base = (blk * SCAN_BLOCK) as u32;
            let mut newlines = Vec::new();
            let mut quotes = Vec::new();
            for (i, &b) in bytes.iter().enumerate() {
                match b {
                    b'"' => quotes.push(base + i as u32),
                    b'\n' => newlines.push(base + i as u32),
                    b'\r' => return None,
                    _ => {}
                }
            }
            Some((newlines, quotes))
        })
        .collect();
    let mut blocks = Vec::with_capacity(scanned.len());
    for block in scanned {
        blocks.push(block?);
    }
    // Without a single quote the newlines are the record ends: no second pass.
    if blocks.iter().all(|(_, quotes)| quotes.is_empty()) {
        return Some(blocks.into_iter().flat_map(|(nl, _)| nl).collect());
    }
    let mut starts = Vec::with_capacity(blocks.len());
    let mut in_quotes = false;
    for (_, quotes) in &blocks {
        starts.push(in_quotes);
        if quotes.len() % 2 == 1 {
            in_quotes = !in_quotes;
        }
    }
    // `partial` is a window, not a file: its last record may end inside a
    // quoted field, and the caller re-reads that tail with more bytes.
    if in_quotes && !partial {
        return None;
    }
    let parts: Vec<Option<Vec<u32>>> = blocks
        .par_iter()
        .zip(starts)
        .map(|((newlines, quotes), mut in_quotes)| {
            let mut out = Vec::with_capacity(newlines.len());
            let (mut ni, mut qi) = (0usize, 0usize);
            while ni < newlines.len() || qi < quotes.len() {
                let at_quote = match (newlines.get(ni), quotes.get(qi)) {
                    (Some(n), Some(q)) => q < n,
                    (Some(_), None) => false,
                    (None, Some(_)) => true,
                    (None, None) => break,
                };
                if !at_quote {
                    if !in_quotes {
                        out.push(newlines[ni]);
                    }
                    ni += 1;
                    continue;
                }
                let pos = quotes[qi] as usize;
                if in_quotes {
                    // A doubled quote is content, not a close.
                    if quotes.get(qi + 1) == Some(&(pos as u32 + 1)) {
                        qi += 2;
                        continue;
                    }
                    // A close must be followed by a delimiter or a line end
                    // (or by the end of a window, which continues).
                    if !matches!(data.get(pos + 1), Some(b',') | Some(b'\n') | None)
                        && !(partial && pos + 1 == data.len())
                    {
                        return None;
                    }
                    in_quotes = false;
                } else {
                    let opens_field = match pos.checked_sub(1) {
                        None => true,
                        Some(before) => matches!(data[before], b',' | b'\n'),
                    };
                    if !opens_field {
                        return None;
                    }
                    in_quotes = true;
                }
                qi += 1;
            }
            Some(out)
        })
        .collect();
    let mut newlines = Vec::with_capacity(data.len() / 24 + 16);
    for part in parts {
        newlines.extend(part?);
    }
    Some(newlines)
}

/// Rayon-parallel parse of a "clean" file: split the data region into
/// ~[`BATCH_LINES`]-record batches (all on record boundaries), parse each
/// batch on the pool, then move the chunks into `table` in file order. Clean
/// means [`record_ends`] could prove the dialect, so line splitting matches
/// the record reader's (see `Fields`) and the file is parsed exactly once.
/// Returns [`CleanError::NotClean`] before touching the table when the dialect
/// isn't provable, so the caller can fall back to the record reader.
fn parse_clean(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    types: &[DataType],
    has_header: bool,
) -> std::result::Result<usize, CleanError> {
    if data.len() > u32::MAX as usize {
        return Err(CleanError::NotClean);
    }
    // Locate every record end, or bail to the record reader.
    let Some(newlines) = record_ends(data, false) else {
        return Err(CleanError::NotClean);
    };
    let tbl = db.table_mut_in(db_name, table).map_err(CleanError::Db)?;
    clean_batches(data, &newlines, types, has_header, 0, &mut |chunk| {
        tbl.append_chunk(chunk)
    })
    .map_err(CleanError::Db)
}

/// Whether the batch path can serve these options: it models the default
/// dialect and cell rules only (comma, RFC 4180 quotes, no escape byte, strict
/// errors, no NULL tokens, no forced-empty columns, UTF-8 without a BOM).
fn fast_eligible(data: &[u8], opts: &CsvOptions) -> bool {
    let encoding_ok = match opts.encoding {
        Encoding::Utf8 => true,
        Encoding::Auto => !has_utf16_bom(data) && !data.starts_with(&[0xEF, 0xBB, 0xBF]),
        _ => false,
    };
    encoding_ok
        && opts.delimiter == b','
        && opts.quote == b'"'
        && opts.escape.is_none()
        && opts.errors == ErrorPolicy::Error
        && opts.nullstr.is_empty()
        && opts.force_not_null.is_empty()
        && opts.allow_quoted_nulls
}

fn has_utf16_bom(data: &[u8]) -> bool {
    data.starts_with(&[0xFF, 0xFE]) || data.starts_with(&[0xFE, 0xFF])
}

/// Fast path: one parse pass with the sniffed types. A later row that breaks
/// one of them surfaces as a `Type` error, which the caller retries with the
/// exact pass; the header decision is the sniff's. Drops the table on failure
/// so the retry starts clean.
fn load_sampled(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    schema: &CsvSchema,
) -> Result<usize> {
    let types = &schema.types;
    let names: Vec<(String, DataType)> = schema
        .names
        .iter()
        .cloned()
        .zip(types.iter().copied())
        .collect();
    db.create_table_in(db_name, table, names)?;
    let has_header = schema.options.header == Header::Present;
    let result = match parse_clean(db, db_name, table, data, types, has_header) {
        Ok(n) => Ok(n),
        Err(CleanError::NotClean) => append_rows(db, db_name, table, data, schema).map(|(n, _)| n),
        Err(CleanError::Db(e)) => Err(e),
    };
    match result {
        Ok(n) => Ok(n),
        Err(e) => {
            db.drop_table_in(db_name, table).ok();
            Err(e)
        }
    }
}

/// Append the file's rows to an existing table (identity column mapping).
fn append_rows(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    schema: &CsvSchema,
) -> Result<(usize, Rejects)> {
    let sources = identity_sources(schema);
    let tbl = db.table_mut_in(db_name, table)?;
    read_rows(
        std::io::Cursor::new(data),
        schema,
        &schema.types,
        &sources,
        skip_header(schema),
        0,
        &mut |chunk| tbl.append_chunk(chunk),
    )
}

/// Exact two-pass path: the sniffed schema, then one parse. Creates `table`
/// and drops it again on failure (all-or-nothing).
#[cfg(test)]
fn load_full(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    opts: &CsvOptions,
) -> Result<(usize, Rejects)> {
    load_exact(db, db_name, table, data, opts)
}

fn load_exact(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    opts: &CsvOptions,
) -> Result<(usize, Rejects)> {
    let schema = sniff_csv_exact(data, opts)?;
    let schema = &schema;
    let names: Vec<(String, DataType)> = schema
        .names
        .iter()
        .cloned()
        .zip(schema.types.iter().copied())
        .collect();
    db.create_table_in(db_name, table, names)?;
    // Pass 2: parse + append (record buffer reused).
    match append_rows(db, db_name, table, data, schema) {
        Ok((rows, rejects)) => Ok((rows, rejects)),
        Err(e) => {
            db.drop_table_in(db_name, table).ok();
            Err(e)
        }
    }
}

/// Load CSV bytes into a fresh table (shared by file/string import).
/// All-or-nothing: a failure drops the created table.
fn load_bytes(
    db: &mut PinkDb,
    db_name: &str,
    table: &str,
    data: &[u8],
    opts: &CsvOptions,
) -> Result<(usize, Rejects)> {
    let schema = sniff_csv(data, opts)?;
    // No fast path for a non-default dialect, and none when the sample never
    // saw a column's only values: infer over the whole file instead.
    if !fast_eligible(data, opts) || schema.partial {
        return load_exact(db, db_name, table, data, opts);
    }
    match load_sampled(db, db_name, table, data, &schema) {
        Ok(rows) => Ok((rows, Rejects::default())),
        // A row beyond the sample violated a sampled type (the table is
        // already dropped): rerun with whole-file inference.
        Err(PinkDbError::Type(_)) => load_exact(db, db_name, table, data, opts),
        Err(e) => Err(e),
    }
}

/// An import has no SQL text to log, so a durable database cannot replay it
/// like a statement. Refuse an import inside an open transaction (the
/// checkpoint that would make it durable needs a settled transaction).
fn require_settled_import(db: &PinkDb) -> Result<()> {
    if db.is_durable() && db.in_transaction() {
        return Err(PinkDbError::Execution(
            "cannot import into a durable database inside a transaction; commit or roll \
             back first (a durable import needs its own checkpoint)"
                .to_string(),
        ));
    }
    Ok(())
}

/// Make a successful import durable: in durable mode a checkpoint publishes
/// the imported rows atomically and rolls the WAL, so a crash right after the
/// import returns still sees them. An import that created the table drops it
/// again when the checkpoint fails (a failed import is never half-visible); an
/// append keeps its rows for the next checkpoint.
fn make_import_durable(db: &mut PinkDb, db_name: &str, table: &str, created: bool) -> Result<()> {
    if !db.is_durable() {
        return Ok(());
    }
    match db.checkpoint() {
        Ok(()) => Ok(()),
        Err(e) => {
            if created {
                db.drop_table_in(db_name, table).ok();
            }
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Table targets.
// ---------------------------------------------------------------------------

/// Parse one leading identifier: a bare token (up to `.` or whitespace) or a
/// double-quoted identifier with `""` escapes. Returns the unquoted name and
/// the unconsumed input.
fn take_ident(input: &str) -> std::result::Result<(String, &str), String> {
    let s = input.trim_start();
    if s.starts_with('"') {
        return take_quoted_ident(s);
    }
    let end = s
        .find(|c: char| c == '.' || c.is_whitespace())
        .unwrap_or(s.len());
    if end == 0 {
        return Err("expected an identifier".to_string());
    }
    Ok((s[..end].to_string(), &s[end..]))
}

/// Parse a double-quoted identifier (`""` is an embedded quote). The caller
/// has checked the leading `"`.
fn take_quoted_ident(s: &str) -> std::result::Result<(String, &str), String> {
    let rest = s.strip_prefix('"').expect("caller checked the quote");
    let mut name = String::new();
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '"' {
            if rest[i + 1..].starts_with('"') {
                name.push('"');
                chars.next();
            } else {
                return Ok((name, &rest[i + 1..]));
            }
        } else {
            name.push(c);
        }
    }
    Err("unterminated quoted identifier".to_string())
}

/// Parse the part after `db.`: a quoted identifier, or a bare token up to the
/// next whitespace (dots included, so `a.b.c` splits once into `a` + `b.c`).
fn take_target_tail(s: &str) -> std::result::Result<(String, &str), String> {
    let s = s.trim_start();
    if s.starts_with('"') {
        return take_quoted_ident(s);
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    if end == 0 {
        return Err("expected a table name after `.`".to_string());
    }
    Ok((s[..end].to_string(), &s[end..]))
}

/// Parse a table target that may be `db.table`, honoring double-quoted
/// identifiers. Returns the parsed parts (both unquoted); a spec that is not
/// a well-formed two-part name is a single unqualified table name. Whether
/// the first part is a *database* is the caller's decision (see
/// [`resolve_import_target`]).
pub fn parse_table_target(spec: &str) -> Result<(Option<String>, String)> {
    let spec = spec.trim();
    let (first, rest) = take_ident(spec).map_err(PinkDbError::Parse)?;
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix('.')
        && let Ok((second, tail)) = take_target_tail(rest)
        && tail.trim().is_empty()
    {
        return Ok((Some(first), second));
    } else if rest.is_empty() {
        // A single (possibly quoted) name.
        return Ok((None, first));
    }
    Ok((None, spec.to_string()))
}

/// Resolve an import target: `db.table` goes to that database only when `db`
/// names an EXISTING database — otherwise the whole string is the table name
/// (so a dotted file stem like `data.2024` stays one name). A plain name goes
/// to the session's current database.
fn resolve_import_target(db: &PinkDb, table: Option<&str>, path: &str) -> Result<(String, String)> {
    let spec = match table {
        Some(t) => t.to_string(),
        None => default_table_name(path),
    };
    match parse_table_target(&spec)? {
        (Some(q), name) if db.database_exists(&q) => Ok((q.to_ascii_lowercase(), name)),
        (Some(q), name) => Ok((db.current_db().to_string(), format!("{q}.{name}"))),
        (None, name) => Ok((db.current_db().to_string(), name)),
    }
}

/// Map a file's fields onto an existing table's columns.
///
/// A file with a header row (or with `names=`) matches by column name,
/// case-insensitively: a table column the file does not have is NULL, and a
/// file column the table does not have follows the error policy. A file
/// without a header maps positionally.
fn map_targets(
    schema: &CsvSchema,
    target: &[(String, DataType)],
    table: &str,
) -> Result<(Vec<DataType>, Vec<Source>, Rejects)> {
    let by_name = schema.options.header == Header::Present;
    let forced: Vec<String> = schema
        .options
        .force_not_null
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    let mut used = vec![false; schema.names.len()];
    let mut sources = Vec::with_capacity(target.len());
    for (pos, (name, _)) in target.iter().enumerate() {
        let idx = if by_name {
            schema
                .names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
        } else if pos < schema.names.len() {
            Some(pos)
        } else {
            None
        };
        match idx {
            Some(i) => {
                used[i] = true;
                sources.push(Source::Field {
                    idx: i,
                    keep_empty: forced.contains(&schema.names[i].to_ascii_lowercase()),
                });
            }
            None => sources.push(Source::Null),
        }
    }
    let mut rejects = Rejects::default();
    for (i, name) in schema.names.iter().enumerate() {
        if used[i] {
            continue;
        }
        let reason = format!("no column \"{name}\" in table \"{table}\"");
        match schema.options.errors {
            ErrorPolicy::Error => {
                return Err(PinkDbError::Catalog(format!(
                    "CSV column \"{name}\" has {reason}"
                )));
            }
            ErrorPolicy::Warn => rejects.push(1, Some(name), reason),
        }
    }
    let types = target.iter().map(|(_, t)| *t).collect();
    Ok((types, sources, rejects))
}

// ---------------------------------------------------------------------------
// Public import API.
// ---------------------------------------------------------------------------

/// The outcome of one import.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Rows appended to the table.
    pub rows: usize,
    /// Rows skipped by `errors=warn` (empty in strict mode).
    pub rejects: Rejects,
}

/// Import a CSV file into `table` (default: file stem) with defaults.
/// Equivalent to [`import_csv_opts`] with [`CsvOptions::default`].
pub fn import_csv(db: &mut PinkDb, path: &str, table: Option<&str>) -> Result<usize> {
    import_csv_opts(db, path, table, &CsvOptions::default()).map(|r| r.rows)
}

/// Import a CSV file into `table` (default: file stem) with explicit options.
///
/// `table` may be `db.table` to target a specific database; otherwise the
/// current database is used. When the table exists, the file's fields are
/// matched into it by header name (positionally without a header); when it
/// does not, it is created from the sniffed schema. On a durable database the
/// import is durable before returning (atomic checkpoint publish + WAL roll).
pub fn import_csv_opts(
    db: &mut PinkDb,
    path: &str,
    table: Option<&str>,
    opts: &CsvOptions,
) -> Result<ImportReport> {
    let data = read_import_bytes(path)?;
    import_into(db, table, path, &data, opts)
}

/// Import CSV text (header + rows) — used by tests and the REPL. Durable
/// databases checkpoint the import before returning, like [`import_csv`].
/// `table` may be `db.table` to target a specific database.
pub fn import_csv_string(db: &mut PinkDb, table: &str, csv_text: &str) -> Result<usize> {
    import_csv_string_opts(db, table, csv_text, &CsvOptions::default()).map(|r| r.rows)
}

/// [`import_csv_string`] with explicit options.
pub fn import_csv_string_opts(
    db: &mut PinkDb,
    table: &str,
    csv_text: &str,
    opts: &CsvOptions,
) -> Result<ImportReport> {
    import_into(db, Some(table), "", csv_text.as_bytes(), opts)
}

fn import_into(
    db: &mut PinkDb,
    table: Option<&str>,
    path_hint: &str,
    data: &[u8],
    opts: &CsvOptions,
) -> Result<ImportReport> {
    let (db_name, table_name) = resolve_import_target(db, table, path_hint)?;
    require_settled_import(db)?;
    // Sniff before touching the catalog: a bad file never creates a table.
    let schema = sniff_csv(data, opts)?;
    if let Ok(existing) = db.table_in(&db_name, &table_name) {
        let target = existing.schema();
        let (types, sources, mut rejects) = map_targets(&schema, &target, &table_name)?;
        // A bad row must leave the table as it was: the engine's statements are
        // atomic, and chunks are Arc-shared, so the store snapshot is cheap.
        let snapshot = crate::procedure::boxed_snapshot(db);
        let result = {
            let tbl = db.table_mut_in(&db_name, &table_name)?;
            read_rows(
                std::io::Cursor::new(data),
                &schema,
                &types,
                &sources,
                skip_header(&schema),
                0,
                &mut |chunk| tbl.append_chunk(chunk),
            )
        };
        let (rows, mut row_rejects) = match result {
            Ok(done) => done,
            Err(e) => {
                db.savepoint_restore(snapshot);
                return Err(e);
            }
        };
        rejects.append(&mut row_rejects);
        make_import_durable(db, &db_name, &table_name, false)?;
        return Ok(ImportReport { rows, rejects });
    }
    let (rows, rejects) = load_bytes(db, &db_name, &table_name, data, opts)?;
    make_import_durable(db, &db_name, &table_name, true)?;
    Ok(ImportReport { rows, rejects })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_csv_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rows_of(table: &crate::storage::DataTable) -> Vec<Vec<Value>> {
        let width = table.schema().len();
        (0..table.num_rows())
            .map(|r| {
                (0..width)
                    .map(|c| table.get(r, c).unwrap().unwrap_or(Value::Null))
                    .collect()
            })
            .collect()
    }

    fn table_rows(db: &PinkDb, name: &str) -> Vec<Vec<Value>> {
        rows_of(db.table(name).expect("table"))
    }

    #[test]
    fn hugeint_cells_name_the_boundary() {
        let err = field_value("1", DataType::Hugeint).expect_err("no HUGEINT CSV layout yet");
        assert!(
            err.to_string()
                .contains("HUGEINT is not yet supported in CSV"),
            "{err}"
        );
        let err = field_value("1", DataType::Uhugeint).expect_err("no UHUGEINT CSV layout yet");
        assert!(
            err.to_string()
                .contains("UHUGEINT is not yet supported in CSV"),
            "{err}"
        );
    }

    fn table_types(db: &PinkDb, name: &str) -> Vec<DataType> {
        db.table(name)
            .expect("table")
            .schema()
            .iter()
            .map(|(_, t)| *t)
            .collect()
    }

    fn err_kind(e: &PinkDbError) -> std::mem::Discriminant<PinkDbError> {
        std::mem::discriminant(e)
    }

    /// Options from `key=value` pairs, through the shared option table.
    fn opts(pairs: &[(&str, &str)]) -> CsvOptions {
        let mut o = CsvOptions::default();
        for (k, v) in pairs {
            o.apply(k, v).unwrap_or_else(|e| panic!("{k}={v}: {e}"));
        }
        o
    }

    fn v(s: &str) -> Value {
        Value::Varchar(s.to_string())
    }

    /// UTF-16 code units of `text`, with an optional byte-order mark.
    fn utf16(text: &str, le: bool, bom: bool) -> Vec<u8> {
        let mut out = Vec::new();
        if bom {
            out.extend_from_slice(if le { &[0xFF, 0xFE] } else { &[0xFE, 0xFF] });
        }
        for unit in text.encode_utf16() {
            out.extend_from_slice(&if le {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            });
        }
        out
    }

    /// Import raw bytes (any encoding) into `table`.
    fn import_bytes(db: &mut PinkDb, table: &str, bytes: &[u8], o: &CsvOptions) -> Result<usize> {
        import_into(db, Some(table), "", bytes, o).map(|r| r.rows)
    }

    #[test]
    fn inference_picks_expected_types() {
        assert_eq!(infer_column(&["1", "2", "3"]), DataType::Integer);
        assert_eq!(infer_column(&["1", "9999999999"]), DataType::Bigint);
        assert_eq!(infer_column(&["1", "2.5"]), DataType::Double);
        assert_eq!(infer_column(&["true", "False"]), DataType::Boolean);
        assert_eq!(infer_column(&["2024-01-02", "2024-02-03"]), DataType::Date);
        assert_eq!(
            infer_column(&["2024-01-02 10:00:00", "2024-02-03 11:30:00"]),
            DataType::Timestamp
        );
        assert_eq!(infer_column(&["x", "1"]), DataType::Varchar);
        // All-empty and empty input infer VARCHAR (DuckDB's rule).
        assert_eq!(infer_column(&["", "  "]), DataType::Varchar);
        assert_eq!(infer_column(&[]), DataType::Varchar);
        // A DATE never widens to TIMESTAMP unless the data needs it.
        assert_eq!(
            schema_types(infer_schema(
                &["d".to_string()],
                &[vec!["2024-01-02".to_string()]]
            )),
            vec![DataType::Date]
        );
    }

    fn schema_types(types: Vec<DataType>) -> Vec<DataType> {
        types
    }

    #[test]
    fn bigint_and_all_empty_column() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", "big,note\n9999999999,\n").unwrap();
        assert_eq!(
            table_types(&db, "t"),
            vec![DataType::Bigint, DataType::Varchar]
        );
        assert_eq!(
            table_rows(&db, "t"),
            vec![vec![Value::Bigint(9_999_999_999), Value::Null]]
        );
    }

    #[test]
    fn csv_helpers_match_import_semantics() {
        let text = "a,b\n1,x\n2,\n";
        let mut db = PinkDb::new();
        let (fast, _) = load_bytes(
            &mut db,
            "main",
            "fast",
            text.as_bytes(),
            &CsvOptions::default(),
        )
        .unwrap();
        let (exact, _) = load_full(
            &mut db,
            "main",
            "exact",
            text.as_bytes(),
            &CsvOptions::default(),
        )
        .unwrap();
        assert_eq!(fast, exact);
        assert_eq!(table_rows(&db, "fast"), table_rows(&db, "exact"));
        let mut db2 = PinkDb::new();
        assert_eq!(import_csv_string(&mut db2, "t", text).unwrap(), fast);
        assert_eq!(table_rows(&db2, "t"), table_rows(&db, "fast"));
    }

    #[test]
    fn import_csv_file_defaults_to_stem() {
        let dir = scratch_dir("stem");
        let path = dir.join("sales-2024.csv");
        std::fs::write(&path, "id,n\n1,a\n").unwrap();
        let mut db = PinkDb::new();
        assert_eq!(
            import_csv(&mut db, path.to_str().unwrap(), None).unwrap(),
            1
        );
        assert_eq!(
            table_rows(&db, "sales-2024"),
            vec![vec![Value::Integer(1), v("a")]]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parse_table_target_honors_quoting_and_single_split() {
        assert_eq!(parse_table_target("t").unwrap(), (None, "t".to_string()));
        assert_eq!(
            parse_table_target("db.t").unwrap(),
            (Some("db".to_string()), "t".to_string())
        );
        assert_eq!(
            parse_table_target("\"my db\".\"my \"\"t\"\"\"").unwrap(),
            (Some("my db".to_string()), "my \"t\"".to_string())
        );
        // Only the first dot splits; the tail stays whole.
        assert_eq!(
            parse_table_target("a.b.c").unwrap(),
            (Some("a".to_string()), "b.c".to_string())
        );
        // Anything that is not a clean two-part name is one table name.
        assert_eq!(
            parse_table_target("x y").unwrap(),
            (None, "x y".to_string())
        );
        assert!(parse_table_target("\"open").is_err());
    }

    #[test]
    fn dotted_import_names_stay_whole_unless_the_left_side_is_a_database() {
        let mut db = PinkDb::new();
        db.execute("CREATE DATABASE db1;").unwrap();
        // `db1.t` resolves into db1 …
        import_csv_string(&mut db, "db1.t", "a\n1\n").unwrap();
        assert_eq!(
            rows_of(db.table_in("db1", "t").unwrap()),
            vec![vec![Value::Integer(1)]]
        );
        // … while an unknown left side keeps the whole dotted name.
        import_csv_string(&mut db, "data.2024", "a\n1\n").unwrap();
        assert_eq!(table_rows(&db, "data.2024"), vec![vec![Value::Integer(1)]]);
    }

    #[test]
    fn ragged_rows_treat_missing_as_null() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", "a,b,c\n1,2,3\n4,5\n").unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                vec![Value::Integer(4), Value::Integer(5), Value::Null],
            ]
        );
    }

    #[test]
    fn ragged_rows_reject_extra_fields() {
        let mut db = PinkDb::new();
        let err = import_csv_string(&mut db, "t", "a,b\n1,2,3\n").unwrap_err();
        assert!(err.to_string().contains("3 fields"), "{err}");
        assert!(db.table("t").is_err(), "no table is left behind");
    }

    /// The structural error names the row that broke it on both parse paths:
    /// the clean byte splitter and the record reader (a quoted earlier row
    /// forces the latter).
    #[test]
    fn structural_errors_carry_the_line_and_the_csv_kind() {
        let mut db = PinkDb::new();
        let err = import_csv_string(&mut db, "t", "a,b\n1,2\n3,4\n5,6,7\n").unwrap_err();
        assert_eq!(crate::agent::error_kind(&err), "csv");
        assert_eq!(
            err.to_string(),
            "CSV error on line 4: row has 3 fields but the header has 2"
        );
        let err = import_csv_string(&mut db, "u", "a,b\n\"x,y\",2\n3,4\n5,6,7\n").unwrap_err();
        assert_eq!(crate::agent::error_kind(&err), "csv");
        assert_eq!(
            err.to_string(),
            "CSV error on line 4: row has 3 fields but the header has 2"
        );
    }

    #[test]
    fn quoted_cells_keep_their_whitespace() {
        // DuckDB leaves a quoted cell's spaces alone, so a quoted `" 1 "` is
        // still a number while `"  x  "` keeps them.
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", "a,b\n\" 1 \",\"  x  \"\n").unwrap();
        assert_eq!(
            table_types(&db, "t"),
            vec![DataType::Integer, DataType::Varchar]
        );
        assert_eq!(
            table_rows(&db, "t"),
            vec![vec![Value::Integer(1), v("  x  ")]]
        );
    }

    #[test]
    fn quoted_fields_survive_chunking() {
        let rows = CHUNK_SIZE + 10;
        let mut csv = String::from("id,text\n");
        for i in 0..rows {
            csv.push_str(&format!("{i},\"line\n{i}, with comma\"\n"));
        }
        let mut db = PinkDb::new();
        assert_eq!(import_csv_string(&mut db, "t", &csv).unwrap(), rows);
        let got = table_rows(&db, "t");
        assert_eq!(got.len(), rows);
        let last = rows - 1;
        assert_eq!(
            got[last],
            vec![
                Value::Integer(last as i32),
                v(&format!("line\n{last}, with comma"))
            ]
        );
    }

    #[test]
    fn crlf_header_only_and_unicode() {
        let mut db = PinkDb::new();
        import_csv_string(&mut db, "t", "a,b\r\n1,x\r\n2,\r\n").unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![
                vec![Value::Integer(1), v("x")],
                vec![Value::Integer(2), Value::Null]
            ]
        );
        // A header-only file creates the table with no rows.
        import_csv_string(&mut db, "h", "a,b\r\n").unwrap();
        assert_eq!(table_rows(&db, "h").len(), 0);
        assert_eq!(table_types(&db, "h").len(), 2);
        // Unicode + surrounding whitespace (unquoted cells are trimmed).
        import_csv_string(&mut db, "u", "a\n Héllo \n").unwrap();
        assert_eq!(table_rows(&db, "u"), vec![vec![v("Héllo")]]);
    }

    #[test]
    fn invalid_utf8_errors_cleanly() {
        let dir = scratch_dir("badutf8");
        let path = dir.join("bad.csv");
        std::fs::write(&path, b"a\n\xff\n").unwrap();
        let mut db = PinkDb::new();
        let err = import_csv(&mut db, path.to_str().unwrap(), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid UTF-8"), "{err}");
        assert!(db.table("bad").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tail_violation_falls_back_to_whole_file_inference() {
        let mut csv = String::from("n\n");
        for i in 0..INFER_SAMPLE_ROWS + 5 {
            csv.push_str(&format!("{i}\n"));
        }
        csv.push_str("tail\n");
        let mut db = PinkDb::new();
        let rows = import_csv_string(&mut db, "t", &csv).unwrap();
        assert_eq!(rows, INFER_SAMPLE_ROWS + 6);
        assert_eq!(table_types(&db, "t"), vec![DataType::Varchar]);
        assert_eq!(
            table_rows(&db, "t").last().unwrap(),
            &vec![v("tail")],
            "the tail row is imported as text"
        );
    }

    #[test]
    fn empty_in_sample_column_falls_back() {
        let mut csv = String::from("a,b\n");
        for i in 0..INFER_SAMPLE_ROWS {
            csv.push_str(&format!("{i},\n"));
        }
        csv.push_str("1,text\n");
        let mut db = PinkDb::new();
        assert_eq!(
            import_csv_string(&mut db, "t", &csv).unwrap(),
            INFER_SAMPLE_ROWS + 1
        );
        assert_eq!(
            table_types(&db, "t"),
            vec![DataType::Integer, DataType::Varchar]
        );
        assert_eq!(
            table_rows(&db, "t").last().unwrap(),
            &vec![Value::Integer(1), v("text")]
        );
    }

    #[test]
    fn fallback_drops_table_on_structural_error() {
        let mut db = PinkDb::new();
        // A row with a field too many: the batch parser rejects it, so the
        // exact pass runs and its error drops the table it created.
        let err = load_exact(
            &mut db,
            "main",
            "t",
            b"a,b\n1,2\n3,4,5\n",
            &opts(&[("header", "true")]),
        )
        .unwrap_err();
        assert_eq!(
            err_kind(&err),
            err_kind(&PinkDbError::Csv(Box::new(crate::types::CsvError {
                message: String::new(),
                line: None,
                column: None,
            })))
        );
        assert_eq!(
            err.to_string(),
            "CSV error on line 3: row has 3 fields but the header has 2"
        );
        assert!(db.table("t").is_err(), "the created table is dropped");
    }

    #[test]
    fn fast_path_matches_exact_two_pass() {
        let mut long = String::from("a,b\n");
        for i in 0..5000 {
            long.push_str(&format!("{i},{}\n", i % 7));
        }
        let corpus: Vec<String> = [
            "a,b\n1,x\n2,\n",
            "a,b\n1,2\n\n3,4\n",
            "a,b\n1,2\n   \n3,4\n",
            "a,b\n\n",
            "a,b\n1,2\n3,4",
            "a,b\r\n1,2\r\n",
            "a,b\n\"x,y\",2\n",
            "a,b\n1,\"\"\n",
            "a,b\n1,2,3\n",
            "a,b\n1\n",
            "a\n2024-13-01\n",
            "big,note\n9999999999,\n",
            "a,b\n,2\n",
            "a,b\n1,true\n2,false\n",
            "a,b\n1,2024-01-02\n2,2024-02-03\n",
            "a,b\n1,2024-01-02 10:00:00\n",
        ]
        .into_iter()
        .map(str::to_string)
        .chain([long])
        .collect();
        for (i, text) in corpus.iter().enumerate() {
            let mut fast = PinkDb::new();
            let mut exact = PinkDb::new();
            let a = load_bytes(
                &mut fast,
                "main",
                "t",
                text.as_bytes(),
                &CsvOptions::default(),
            );
            let b = load_full(
                &mut exact,
                "main",
                "t",
                text.as_bytes(),
                &CsvOptions::default(),
            );
            match (a, b) {
                (Ok((rows_a, _)), Ok((rows_b, _))) => {
                    assert_eq!(rows_a, rows_b, "case {i}: row count\n{text:?}");
                    assert_eq!(
                        table_types(&fast, "t"),
                        table_types(&exact, "t"),
                        "case {i}: schema\n{text:?}"
                    );
                    assert_eq!(
                        table_rows(&fast, "t"),
                        table_rows(&exact, "t"),
                        "case {i}: values\n{text:?}"
                    );
                }
                (Err(a), Err(b)) => assert_eq!(
                    err_kind(&a),
                    err_kind(&b),
                    "case {i}: error kind ({a} vs {b})\n{text:?}"
                ),
                (a, b) => panic!("case {i}: one path failed: {a:?} vs {b:?}\n{text:?}"),
            }
        }
    }

    #[test]
    fn clean_parse_guards_and_batching() {
        let types = vec![DataType::Integer, DataType::Varchar];
        let mut db = PinkDb::new();
        db.create_table_in(
            "main",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        let n = parse_clean(&mut db, "main", "t", b"a,b\n1,x\n2,y\n", &types, true).unwrap();
        assert_eq!(n, 2);
        // RFC 4180 quoting is clean: a comma inside quotes stays in the field
        // and a quoted empty is NULL, the same as the record reader reads it.
        db.create_table_in(
            "main",
            "q",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        let quoted = b"a,b\n1,\"x,y\"\n2,\"\"\n";
        assert_eq!(
            parse_clean(&mut db, "main", "q", quoted, &types, true).unwrap(),
            2
        );
        assert_eq!(
            table_rows(&db, "q"),
            vec![
                vec![Value::Integer(1), v("x,y")],
                vec![Value::Integer(2), Value::Null],
            ]
        );
        // CR, and quoting the record reader reads differently, are not
        // provable from line splits: the caller must use the record reader.
        for not_clean in [
            &b"a,b\r\n1,x\r\n"[..],
            b"a,b\n1,x\"y\n",
            b"a,b\n1,\"x\n",
            b"a,b\n1,\"x\"y\n",
        ] {
            assert!(
                matches!(
                    parse_clean(&mut db, "main", "q", not_clean, &types, true),
                    Err(CleanError::NotClean)
                ),
                "{:?}",
                String::from_utf8_lossy(not_clean)
            );
        }
        // More than one batch, header on and off: every row lands exactly once.
        db.create_table_in(
            "main",
            "batch",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        let rows = BATCH_LINES * 2 + 3;
        let mut csv = String::from("a,b\n");
        for i in 0..rows {
            csv.push_str(&format!("{i},r\n"));
        }
        assert_eq!(
            parse_clean(&mut db, "main", "batch", csv.as_bytes(), &types, true).unwrap(),
            rows
        );
        let counted: i64 = table_rows(&db, "batch")
            .iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i64::from(i),
                _ => 0,
            })
            .sum();
        assert_eq!(counted, (0..rows as i64).sum::<i64>());
        // Without a header, the first line is data.
        db.create_table_in(
            "main",
            "t2",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        assert_eq!(
            parse_clean(&mut db, "main", "t2", b"1,x\n2,y\n", &types, false).unwrap(),
            2
        );
    }

    #[test]
    fn durable_import_checkpoints_and_survives_reopen() {
        let dir = scratch_dir("durable");
        let path = dir.join("db.pink");
        let path = path.to_str().unwrap();
        let mut db = PinkDb::open_durable(path).unwrap();
        assert_eq!(import_csv_string(&mut db, "t", "a,b\n1,x\n").unwrap(), 1);
        drop(db);
        let mut db = PinkDb::open_durable(path).unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![vec![Value::Integer(1), v("x")]],
            "the import is durable before it returns"
        );
        // Inside a transaction an import is refused (it could not checkpoint).
        db.execute("BEGIN;").unwrap();
        let err = import_csv_string(&mut db, "t", "a,b\n2,y\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("inside a transaction"), "{err}");
        db.execute("ROLLBACK;").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `types` / `dtypes` / `column_types` override inferred types, and
    /// `columns` gives every name and type; both take DuckDB's struct-literal
    /// text, and only `columns` also accepts it as a struct (a list is a
    /// binder error there).
    #[test]
    fn options_accept_column_types_and_columns() {
        let mut o = CsvOptions::default();
        o.apply("types", "{DAY_ID: DATE, AMT: DECIMAL(10,2)}")
            .unwrap();
        assert_eq!(
            o.column_types,
            vec![
                (Some("DAY_ID".to_string()), DataType::Date),
                (
                    Some("AMT".to_string()),
                    DataType::Decimal {
                        precision: 10,
                        scale: 2
                    }
                ),
            ]
        );
        o.apply("dtypes", "['DATE', 'VARCHAR']").unwrap();
        assert_eq!(
            o.column_types,
            vec![(None, DataType::Date), (None, DataType::Varchar)],
            "the list form gives the types by position"
        );
        o.apply("column_types", "{'a': 'INTEGER'}").unwrap();
        assert_eq!(
            o.column_types,
            vec![(Some("a".to_string()), DataType::Integer)]
        );
        o.apply("columns", "{x: INT, y: VARCHAR}").unwrap();
        assert_eq!(
            o.columns,
            Some((
                vec!["x".to_string(), "y".to_string()],
                vec![DataType::Integer, DataType::Varchar]
            ))
        );
        // An unknown type name and a shape that is not a literal are refused
        // precisely, and `columns` takes only the struct form.
        assert!(o.apply("types", "{a: WIDGET}").is_err());
        assert!(o.apply("types", "DATE").is_err());
        let err = o.apply("columns", "['INT']").unwrap_err().to_string();
        assert_eq!(err, "read_csv columns requires a struct as input");
    }

    #[test]
    fn sniff_applies_type_overrides_and_explicit_columns() {
        // An override rewrites only the named column; the rest is inferred.
        let mut o = CsvOptions::default();
        o.apply("types", "{a: VARCHAR}").unwrap();
        let schema = sniff_csv(b"a,b\n1,2\n3,4\n", &o).unwrap();
        assert_eq!(schema.names, vec!["a", "b"]);
        assert_eq!(schema.types, vec![DataType::Varchar, DataType::Integer]);
        assert_eq!(
            reader_rows(b"a,b\n1,2\n3,4\n", &schema),
            vec![
                vec![v("1"), Value::Integer(2)],
                vec![v("3"), Value::Integer(4)],
            ]
        );
        // DuckDB's error for a name the file does not have.
        let mut o = CsvOptions::default();
        o.apply("types", "{zz: VARCHAR}").unwrap();
        let err = sniff_csv(b"a,b\n1,2\n", &o).unwrap_err().to_string();
        assert_eq!(
            err,
            "COLUMN_TYPES error: Columns with names: \"zz\" do not exist in the CSV File"
        );
        // `columns` reads with the given names and types: no inference, and
        // the first row is the header.
        let mut o = CsvOptions::default();
        o.apply("columns", "{x: INTEGER, y: VARCHAR}").unwrap();
        let schema = sniff_csv(b"a,b\n1,hello\n2,world\n", &o).unwrap();
        assert_eq!(schema.names, vec!["x", "y"]);
        assert_eq!(schema.types, vec![DataType::Integer, DataType::Varchar]);
        assert_eq!(schema.options.header, Header::Present);
        assert_eq!(
            reader_rows(b"a,b\n1,hello\n2,world\n", &schema),
            vec![
                vec![Value::Integer(1), v("hello")],
                vec![Value::Integer(2), v("world")],
            ]
        );
    }

    #[derive(Debug)]
    struct TestFetcher;

    impl UrlFetcher for TestFetcher {
        fn supports(&self, url: &str) -> bool {
            url.starts_with("test://")
        }

        fn fetch(&self, url: &str) -> std::result::Result<Vec<u8>, String> {
            if url.ends_with("x.csv") {
                Ok(b"a,b\n1,x\n".to_vec())
            } else {
                Err("not found".to_string())
            }
        }
    }

    #[test]
    fn import_accepts_urls_through_the_registered_fetcher() {
        register_global_url_fetcher(Arc::new(TestFetcher));
        let mut db = PinkDb::new();
        assert_eq!(
            import_csv(&mut db, "test://host/x.csv", Some("t")).unwrap(),
            1
        );
        assert_eq!(table_rows(&db, "t"), vec![vec![Value::Integer(1), v("x")]]);
        let err = import_csv(&mut db, "test://host/missing.csv", Some("u"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "{err}");
        let err = import_csv(&mut db, "ftp://host/x.csv", Some("v"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no URL fetcher"), "{err}");
    }

    // -----------------------------------------------------------------------
    // Wave 76: options.
    // -----------------------------------------------------------------------

    #[test]
    fn options_apply_names_and_errors() {
        let mut o = CsvOptions::default();
        o.apply("delimiter", "|").unwrap();
        assert_eq!(o.delimiter, b'|');
        o.apply("sep", ";").unwrap();
        assert_eq!(o.delimiter, b';');
        o.apply("delimiter", "\\t").unwrap();
        assert_eq!(o.delimiter, b'\t');
        o.apply("quote", "'").unwrap();
        assert_eq!(o.quote, b'\'');
        o.apply("escape", "\\").unwrap();
        assert_eq!(o.escape, Some(b'\\'));
        o.apply("escape", "").unwrap();
        assert_eq!(o.escape, None);
        o.apply("nullstr", "NA, N/A ,").unwrap();
        assert_eq!(o.nullstr, vec!["NA", "N/A"]);
        o.apply("names", "a,b").unwrap();
        assert_eq!(o.names, Some(vec!["a".to_string(), "b".to_string()]));
        o.apply("header", "false").unwrap();
        assert_eq!(o.header, Header::Absent);
        o.apply("header", "auto").unwrap();
        assert_eq!(o.header, Header::Auto);
        o.apply("errors", "warn").unwrap();
        assert_eq!(o.errors, ErrorPolicy::Warn);
        o.apply("ignore_errors", "false").unwrap();
        assert_eq!(o.errors, ErrorPolicy::Error);
        o.apply("allow_quoted_nulls", "false").unwrap();
        assert!(!o.allow_quoted_nulls);
        o.apply("encoding", "UTF-16BE").unwrap();
        assert_eq!(o.encoding, Encoding::Utf16Be);

        for (k, want) in [
            ("bogus", "unknown CSV option `bogus`"),
            ("all_varchar", "not supported yet"),
            ("encoding", "unknown encoding"),
            ("header", "expects auto, true or false"),
            ("delimiter", "exactly one byte"),
        ] {
            let value = if k == "encoding" {
                "ebcdic"
            } else if k == "header" {
                "maybe"
            } else if k == "delimiter" {
                ";;"
            } else {
                "1"
            };
            let err = CsvOptions::default()
                .apply(k, value)
                .unwrap_err()
                .to_string();
            assert!(err.contains(want), "{k}: {err}");
        }
    }

    #[test]
    fn delimiter_quote_and_escape_options() {
        let mut db = PinkDb::new();
        import_csv_string_opts(
            &mut db,
            "semi",
            "id;name\n1;'a;b'\n",
            &opts(&[("delimiter", ";"), ("quote", "'")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "semi"),
            vec![vec![Value::Integer(1), v("a;b")]]
        );

        import_csv_string_opts(
            &mut db,
            "esc",
            "id;name\n1;'a\\'b'\n",
            &opts(&[("delimiter", ";"), ("quote", "'"), ("escape", "\\")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "esc"),
            vec![vec![Value::Integer(1), v("a'b")]]
        );

        // A tab delimiter, spelled both ways.
        import_csv_string_opts(
            &mut db,
            "tab",
            "a\tb\n1\tx\n",
            &opts(&[("delimiter", "\\t")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "tab"),
            vec![vec![Value::Integer(1), v("x")]]
        );
        import_csv_string_opts(
            &mut db,
            "tab2",
            "a\tb\n2\ty\n",
            &opts(&[("delimiter", "\t")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "tab2"),
            vec![vec![Value::Integer(2), v("y")]]
        );
    }

    #[test]
    fn latin1_windows1252_and_utf16_encodings() {
        let mut db = PinkDb::new();
        // latin-1: 0xE9 é, 0xFC ü.
        import_bytes(
            &mut db,
            "l1",
            b"name,city\nAndr\xe9,Z\xfcrich\n",
            &opts(&[("encoding", "latin-1")]),
        )
        .unwrap();
        assert_eq!(table_rows(&db, "l1"), vec![vec![v("André"), v("Zürich")]]);
        // windows-1252: 0x80 is € (which latin-1 would read as U+0080).
        import_bytes(
            &mut db,
            "cp",
            b"price\n\x80\n",
            &opts(&[("encoding", "windows-1252")]),
        )
        .unwrap();
        assert_eq!(table_rows(&db, "cp"), vec![vec![v("€")]]);
        // A UTF-8 BOM is stripped, also under `auto`.
        import_bytes(
            &mut db,
            "bom",
            b"\xef\xbb\xbfa\n1\n",
            &CsvOptions::default(),
        )
        .unwrap();
        assert_eq!(table_rows(&db, "bom"), vec![vec![Value::Integer(1)]]);
    }

    #[test]
    fn utf16_le_and_be_with_and_without_a_bom() {
        let text = "id,name\n1,André\n";
        let mut db = PinkDb::new();
        for (table, le, bom, encoding) in [
            ("le_bom", true, true, "utf-16le"),
            ("le_bom_auto", true, true, "auto"),
            ("be_bom", false, true, "utf-16be"),
            ("be_bom_auto", false, true, "auto"),
            ("le_plain", true, false, "utf-16le"),
            ("be_plain", false, false, "utf-16be"),
            // An explicit endianness still honors the other BOM.
            ("le_bom_as_be", true, true, "utf-16be"),
        ] {
            let bytes = utf16(text, le, bom);
            import_bytes(&mut db, table, &bytes, &opts(&[("encoding", encoding)]))
                .unwrap_or_else(|e| panic!("{table}: {e}"));
            assert_eq!(
                table_rows(&db, table),
                vec![vec![Value::Integer(1), v("André")]],
                "{table}"
            );
        }
        // A non-BMP character survives the surrogate-pair path, split across
        // decoder reads.
        let mut long = String::from("a\n");
        for _ in 0..(READ_CHUNK / 2) {
            long.push('😀');
        }
        import_bytes(
            &mut db,
            "emoji",
            &utf16(&long, true, true),
            &CsvOptions::default(),
        )
        .unwrap();
        assert_eq!(table_rows(&db, "emoji")[0][0], v(&long[2..]));
    }

    #[test]
    fn header_auto_matches_duckdb() {
        fn type_names(types: &[DataType]) -> Vec<String> {
            types.iter().map(|t| t.to_string()).collect()
        }
        let cases: &[(&str, &[&str], &[&str], bool)] = &[
            (
                "1,2\n3,4\n5,6\n",
                &["column0", "column1"],
                &["INTEGER", "INTEGER"],
                false,
            ),
            (
                "a,b\nc,d\ne,f\n",
                &["a", "b"],
                &["VARCHAR", "VARCHAR"],
                true,
            ),
            (
                "a,b\n1,2\n3,4\n",
                &["a", "b"],
                &["INTEGER", "INTEGER"],
                true,
            ),
            (
                "a,1\nb,2\nc,3\n",
                &["column0", "column1"],
                &["VARCHAR", "INTEGER"],
                false,
            ),
            (
                "x,1\ny,z\nw,q\n",
                &["x", "1"],
                &["VARCHAR", "VARCHAR"],
                true,
            ),
            ("a\nb\nc\n", &["a"], &["VARCHAR"], true),
            ("1\n2\n3\n", &["column0"], &["INTEGER"], false),
            (
                "a,\nb,c\nd,e\n",
                &["a", "column1"],
                &["VARCHAR", "VARCHAR"],
                true,
            ),
            ("a,b\n", &["a", "b"], &["VARCHAR", "VARCHAR"], true),
            (
                "id,name\n1,ann\n",
                &["id", "name"],
                &["INTEGER", "VARCHAR"],
                true,
            ),
        ];
        for (text, names, types, header) in cases {
            let schema = sniff_csv(text.as_bytes(), &CsvOptions::default()).unwrap();
            assert_eq!(schema.names, *names, "{text:?}");
            assert_eq!(type_names(&schema.types), *types, "{text:?}");
            let resolved = if *header {
                Header::Present
            } else {
                Header::Absent
            };
            assert_eq!(schema.options.header, resolved, "{text:?}");
        }
    }

    #[test]
    fn names_override_the_header() {
        let mut db = PinkDb::new();
        // header=auto still consumes the detected header row; names rename.
        import_csv_string_opts(&mut db, "t", "a,b\n1,2\n", &opts(&[("names", "x,y")])).unwrap();
        assert_eq!(
            db.table("t")
                .unwrap()
                .schema()
                .iter()
                .map(|(n, _)| n.clone())
                .collect::<Vec<_>>(),
            vec!["x".to_string(), "y".to_string()]
        );
        assert_eq!(
            table_rows(&db, "t"),
            vec![vec![Value::Integer(1), Value::Integer(2)]]
        );
        // header=false keeps the first line as data.
        import_csv_string_opts(
            &mut db,
            "u",
            "1,2\n3,4\n",
            &opts(&[("names", "x,y"), ("header", "false")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "u"),
            vec![
                vec![Value::Integer(1), Value::Integer(2)],
                vec![Value::Integer(3), Value::Integer(4)],
            ]
        );
    }

    #[test]
    fn import_matches_into_an_existing_table_by_header_name() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id INTEGER, name VARCHAR, note VARCHAR);")
            .unwrap();
        // Case-insensitive names; a table column the file lacks is NULL.
        import_csv_string_opts(&mut db, "t", "ID,Name\n1,ann\n", &CsvOptions::default()).unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![vec![Value::Integer(1), v("ann"), Value::Null]]
        );
        // A file column the table lacks follows the error policy.
        let err = import_csv_string_opts(
            &mut db,
            "t",
            "id,name,zzz\n2,bob,1\n",
            &CsvOptions::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("zzz"), "{err}");
        assert_eq!(table_rows(&db, "t").len(), 1, "nothing is appended");
        let report = import_csv_string_opts(
            &mut db,
            "t",
            "id,name,zzz\n2,bob,1\n",
            &opts(&[("errors", "warn")]),
        )
        .unwrap();
        assert_eq!(report.rows, 1);
        assert_eq!(
            report.rejects.to_string(),
            "1 row rejected:\n  line 1, column \"zzz\": no column \"zzz\" in table \"t\"\n"
        );
        assert_eq!(
            table_rows(&db, "t").last().unwrap(),
            &vec![Value::Integer(2), v("bob"), Value::Null]
        );
    }

    #[test]
    fn a_failed_append_leaves_the_existing_table_unchanged() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        import_csv_string(&mut db, "t", "a,b\n1,x\n").unwrap();
        let err = import_csv_string(&mut db, "t", "a,b\n2,y\nbad,z\n").unwrap_err();
        assert!(err.to_string().contains("bad"), "{err}");
        assert_eq!(table_rows(&db, "t"), vec![vec![Value::Integer(1), v("x")]]);
    }

    #[test]
    fn import_without_a_header_maps_positionally() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (id INTEGER, name VARCHAR);")
            .unwrap();
        import_csv_string_opts(
            &mut db,
            "t",
            "1,ann\n2,bob\n",
            &opts(&[("header", "false")]),
        )
        .unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![
                vec![Value::Integer(1), v("ann")],
                vec![Value::Integer(2), v("bob")],
            ]
        );
    }

    #[test]
    fn warn_policy_skips_rows_and_reports() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        let report = import_csv_string_opts(
            &mut db,
            "t",
            "a,b\n1,x\nbad,y\n1,2,3\n4,z\n",
            &opts(&[("errors", "warn")]),
        )
        .unwrap();
        assert_eq!(report.rows, 2);
        assert_eq!(
            table_rows(&db, "t"),
            vec![
                vec![Value::Integer(1), v("x")],
                vec![Value::Integer(4), v("z")]
            ]
        );
        assert_eq!(
            report.rejects.to_string(),
            "2 rows rejected:\n  line 3, column \"a\": cannot parse \"bad\" as INTEGER\n  \
             line 4: row has 3 fields but the header has 2\n"
        );
    }

    #[test]
    fn warn_report_is_capped() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
            .unwrap();
        let mut text = String::from("a,b\n");
        for _ in 0..15 {
            text.push_str("bad,x\n");
        }
        let report =
            import_csv_string_opts(&mut db, "t", &text, &opts(&[("errors", "warn")])).unwrap();
        assert_eq!(report.rows, 0);
        assert_eq!(report.rejects.total, 15);
        assert_eq!(report.rejects.shown.len(), REJECTS_SHOWN);
        let rendered = report.rejects.to_string();
        assert_eq!(rendered.lines().count(), 1 + REJECTS_SHOWN + 1);
        assert!(rendered.ends_with("  … 5 more\n"), "{rendered}");
    }

    #[test]
    fn empty_field_rule_and_null_handling() {
        let header = opts(&[("header", "true")]);
        // Default: an unquoted empty and a quoted empty are both NULL.
        let mut db = PinkDb::new();
        import_csv_string_opts(&mut db, "t", "a,b\n,\"\"\n1,\"x\"\n", &header).unwrap();
        assert_eq!(
            table_rows(&db, "t"),
            vec![
                vec![Value::Null, Value::Null],
                vec![Value::Integer(1), v("x")],
            ]
        );
        // allow_quoted_nulls=false keeps a quoted empty as an empty string,
        // which also makes the column VARCHAR.
        let mut db = PinkDb::new();
        import_csv_string_opts(
            &mut db,
            "q",
            "a,b\n,\"\"\n1,\"x\"\n",
            &opts(&[("header", "true"), ("allow_quoted_nulls", "false")]),
        )
        .unwrap();
        assert_eq!(
            table_types(&db, "q"),
            vec![DataType::Integer, DataType::Varchar]
        );
        assert_eq!(
            table_rows(&db, "q"),
            vec![vec![Value::Null, v("")], vec![Value::Integer(1), v("x")],]
        );
        // nullstr tokens are NULL, quoted or not, and do not affect the type.
        let mut db = PinkDb::new();
        import_csv_string_opts(
            &mut db,
            "n",
            "a\nNA\n\"N/A\"\n5\n",
            &opts(&[("header", "true"), ("nullstr", "NA,N/A")]),
        )
        .unwrap();
        assert_eq!(table_types(&db, "n"), vec![DataType::Integer]);
        assert_eq!(
            table_rows(&db, "n"),
            vec![
                vec![Value::Null],
                vec![Value::Null],
                vec![Value::Integer(5)]
            ]
        );
        // force_not_null: the column's empties stay "" (so it is VARCHAR).
        let mut db = PinkDb::new();
        import_csv_string_opts(
            &mut db,
            "f",
            "a,b\n,1\nx,2\n",
            &opts(&[("header", "true"), ("force_not_null", "a")]),
        )
        .unwrap();
        assert_eq!(table_types(&db, "f")[0], DataType::Varchar);
        assert_eq!(table_rows(&db, "f")[0][0], v(""));
    }

    /// The empty-field rule against the DuckDB binary, when it is installed.
    #[test]
    fn empty_field_rule_matches_duckdb() {
        let bin = std::path::Path::new("/tmp/duckdb-2.0.0/duckdb");
        if !bin.is_file() {
            eprintln!("skipping: {} is not built", bin.display());
            return;
        }
        let dir = scratch_dir("duckdb");
        // Two empty fields: one unquoted, one quoted.
        std::fs::write(dir.join("e.csv"), "a,b\n,\n\"\",x\n1,\"\"\n").unwrap();
        // An all-numeric column with one quoted empty: quoted nulls must not
        // widen it.
        std::fs::write(dir.join("t.csv"), "n\n1\n\"\"\n2\n").unwrap();
        let run = |sql: &str| -> Vec<String> {
            let out = std::process::Command::new(bin)
                .current_dir(&dir)
                .args(["-no-agent", "-csv", "-noheader", "-c", sql])
                .output()
                .expect("duckdb runs");
            assert!(
                out.status.success(),
                "{sql}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(str::to_string)
                .collect()
        };
        let matrix = "SELECT (a IS NULL)::VARCHAR || '|' || (b IS NULL)::VARCHAR || '|' || \
                      coalesce(a, '-') || '|' || coalesce(b, '-') \
                      FROM read_csv('e.csv', header=true, allow_quoted_nulls=false)";
        let duck = run(matrix);
        let mut db = PinkDb::new();
        import_csv_at(
            &mut db,
            "e",
            &dir.join("e.csv"),
            &opts(&[("header", "true"), ("allow_quoted_nulls", "false")]),
        )
        .unwrap();
        let ours: Vec<String> = table_rows(&db, "e")
            .iter()
            .map(|r| {
                format!(
                    "{}|{}|{}|{}",
                    r[0] == Value::Null,
                    r[1] == Value::Null,
                    if r[0] == Value::Null {
                        "-".to_string()
                    } else {
                        r[0].to_string()
                    },
                    if r[1] == Value::Null {
                        "-".to_string()
                    } else {
                        r[1].to_string()
                    },
                )
            })
            .collect();
        assert_eq!(ours, duck, "empty-field rule");

        let width = "SELECT typeof(n) FROM read_csv('t.csv', header=true) LIMIT 1";
        let duck = run(width);
        let mut db = PinkDb::new();
        import_csv_at(
            &mut db,
            "t",
            &dir.join("t.csv"),
            &opts(&[("header", "true")]),
        )
        .unwrap();
        // DuckDB widens an integer CSV column to BIGINT, pinkdb to INTEGER: the
        // agreement under test is VARCHAR-vs-numeric on both sides.
        assert_eq!(duck.len(), 1);
        assert_ne!(duck[0], "VARCHAR");
        assert_ne!(table_types(&db, "t")[0], DataType::Varchar);
        // …and the same file with allow_quoted_nulls=false keeps it VARCHAR.
        let width = "SELECT typeof(n) FROM read_csv('t.csv', header=true, allow_quoted_nulls=false) LIMIT 1";
        let duck = run(width);
        assert_eq!(vec!["VARCHAR".to_string()], duck);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn import_csv_at(
        db: &mut PinkDb,
        table: &str,
        path: &std::path::Path,
        o: &CsvOptions,
    ) -> Result<Report> {
        let bytes = std::fs::read(path)?;
        import_into(db, Some(table), path.to_str().unwrap_or(""), &bytes, o)
    }

    type Report = ImportReport;

    /// A `Read` that hands out three bytes at a time: record, quote and
    /// escape lookahead must survive refills mid-record.
    struct Trickle<'a>(&'a [u8]);

    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = buf.len().min(3).min(self.0.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    #[test]
    fn streaming_reader_survives_small_reads() {
        let text = b"a,b\r\n'x\r\n',1\r\n'don''t',2\r\n";
        let o = opts(&[("header", "true"), ("quote", "'")]);
        assert_eq!(o.quote, b'\'');
        // Only `'` quotes under this option, so the doubled quote inside
        // `'don''t'` is the escape form, and the CRLF inside `'x…'` is data.
        let schema = sniff_csv(text, &o).unwrap();
        let direct = parse_csv_chunks(text, &schema).unwrap();
        let mut streamed = Vec::new();
        let rows = stream_csv_chunks(Trickle(text), &schema, &mut |chunk| {
            streamed.push(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 2);
        let flatten = |chunks: Vec<DataChunk>| -> Vec<Vec<Value>> {
            chunks
                .into_iter()
                .flat_map(|c| {
                    (0..c.num_rows)
                        .map(|r| {
                            (0..c.columns.len())
                                .map(|col| c.columns[col].get(r))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        let direct = flatten(direct);
        assert_eq!(flatten(streamed), direct);
        assert_eq!(
            direct,
            vec![
                vec![v("x\r\n"), Value::Integer(1)],
                vec![v("don't"), Value::Integer(2)]
            ]
        );
    }

    /// Rows of joined chunks, in file order.
    fn flatten(chunks: Vec<DataChunk>) -> Vec<Vec<Value>> {
        chunks
            .into_iter()
            .flat_map(|c| {
                (0..c.num_rows)
                    .map(|r| {
                        (0..c.columns.len())
                            .map(|col| c.columns[col].get(r))
                            .collect()
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Rows the record reader produces for `data` (the reference the batch
    /// paths have to match).
    fn reader_rows(data: &[u8], schema: &CsvSchema) -> Vec<Vec<Value>> {
        let mut out = Vec::new();
        read_rows(
            std::io::Cursor::new(data),
            schema,
            &schema.types,
            &identity_sources(schema),
            schema.options.header == Header::Present,
            0,
            &mut |columns| {
                out.push(DataChunk::new(columns)?);
                Ok(())
            },
        )
        .unwrap();
        flatten(out)
    }

    /// A stream longer than one window takes the batch path window by window
    /// and still agrees with the reader: a quoted field spanning the window,
    /// a quoted comma, and no trailing newline.
    #[test]
    fn streaming_batches_match_the_reader() {
        let long = "x".repeat(STREAM_BLOCK + 7);
        let text = format!("id,note\n1,\"{long}\"\n2,\"a,b\"\n3,plain");
        let data = text.as_bytes();
        assert!(data.len() > STREAM_BLOCK);
        let schema = sniff_csv(data, &CsvOptions::default()).unwrap();
        assert_eq!(schema.types, vec![DataType::Integer, DataType::Varchar]);
        let mut streamed = Vec::new();
        let rows = stream_csv_chunks(std::io::Cursor::new(data), &schema, &mut |chunk| {
            streamed.push(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 3);
        let streamed = flatten(streamed);
        assert_eq!(streamed, reader_rows(data, &schema));
        assert_eq!(
            streamed,
            vec![
                vec![Value::Integer(1), v(&long)],
                vec![Value::Integer(2), v("a,b")],
                vec![Value::Integer(3), v("plain")]
            ]
        );
    }

    /// Small reads, CRLF and a custom quote still fall through to the reader
    /// mid-stream without losing a row or its line numbering; the header is
    /// dropped exactly once even when the batch pass bailed on the first
    /// window.
    #[test]
    fn streaming_head_and_header_survive_a_handover() {
        let text = b"a,b\r\n1,x\r\n2,y\r\n";
        let schema = sniff_csv(text, &CsvOptions::default()).unwrap();
        assert_eq!(schema.options.header, Header::Present);
        let mut streamed = Vec::new();
        let rows = stream_csv_chunks(Trickle(text), &schema, &mut |chunk| {
            streamed.push(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            flatten(streamed),
            vec![
                vec![Value::Integer(1), v("x")],
                vec![Value::Integer(2), v("y")]
            ]
        );
        // A header-only stream keeps its single record out of the rows.
        let mut streamed = Vec::new();
        let rows = stream_csv_chunks(Trickle(b"a,b\r\n"), &schema, &mut |chunk| {
            streamed.push(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 0);
        assert!(flatten(streamed).is_empty());
    }
}
