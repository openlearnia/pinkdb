//! `strftime` and `strptime`, pinned to DuckDB v2.0.0-alpha46057.
//!
//! The supported specifier set is exactly the pinned build's, minus the
//! specifiers this extension intentionally does not carry:
//!
//! * `%z` / `%Z` — time zones (pinkdb timestamps are naive);
//! * `%n` / `%N` — nanosecond fractions (pinkdb timestamps are microsecond);
//! * `%g` — the pinned build's `%g` output is broken (`123` for year 2024);
//!   `%G` is supported and correct;
//! * everything the pinned build itself rejects (`%C %D %e %F %k %l %P %q
//!   %r %R %s %t`) — refused with a clear error, like the pinned build.
//!
//! `strptime` additionally refuses the week-based parse fields `%U %W %V %G`
//! (week-number-to-date arithmetic): DuckDB supports them, pinkdb v1 does
//! not (the supported specifiers cover every format `strftime` can emit for
//! a *date*; round trips stay exact). `%j` and `%Y %m %d` drive the date, as
//! in the pinned build.
//!
//! Parsing rules, all probed against the pinned build:
//! * numeric fields are greedy digit runs, 1..=max digits (`%Y` ≤ 4, `%j` ≤ 3,
//!   `%f` ≤ 6, the rest ≤ 2);
//! * `%y` pivots at 69: `00..=68` → 2000s, `69..=99` → 1900s;
//! * the input must be consumed exactly (no trailing characters) and literal
//!   text (including single spaces) must match byte for byte;
//! * unset fields default to 1900-01-01 00:00:00;
//! * `%j` accepts 1..=366 and rolls over into the next year (2023-366 is
//!   2024-01-01) — the pinned build's behavior;
//! * weekday names (`%a %A`) and weekday numbers (`%u %w`) are validated and
//!   then ignored (the pinned build does not check them against the date);
//! * `%p` without `%I` means 12:00 (PM) / 00:00 (AM).

use crate::civil;

/// The specifiers `strftime` renders (and the byte-for-byte list in the
/// unsupported-specifier error).
pub const SUPPORTED: &str =
    "%a %A %b %B %c %d %f %h %H %I %j %m %M %p %S %T %u %U %V %w %W %x %X %y %Y %G %%";

const MONTHS_ABBR: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const MONTHS_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
/// Index 0 = Sunday, matching `dayofweek` / `%w`.
const DAYS_ABBR: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DAYS_FULL: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

fn unsupported(verb: &str, spec: char) -> String {
    let extra = match spec {
        'z' | 'Z' => " (timestamps are naive: no time zones)",
        'n' | 'N' => " (timestamps are microsecond: no nanosecond fields)",
        'g' => " (use %G)",
        'U' | 'W' | 'V' | 'G' => " (week-based fields are not parsed)",
        _ => "",
    };
    format!("{verb}: unsupported format specifier %{spec}{extra} (supported: {SUPPORTED})")
}

/// Render a naive timestamp with a `strftime` format string.
pub fn strftime(us: i64, format: &str) -> Result<String, String> {
    if format.is_empty() {
        return Err("strftime: empty format string".to_string());
    }
    let (days, tod) = civil::split_timestamp(us);
    let (year, month, day) = civil::civil_from_days(days);
    let secs = tod / 1_000_000;
    let (hour, minute, second) = (secs / 3600, secs % 3600 / 60, secs % 60);
    let micros = tod % 1_000_000;

    let mut out = String::with_capacity(format.len() + 16);
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err("strftime: the format ends with a trailing '%'".to_string());
        };
        match spec {
            'a' => out.push_str(DAYS_ABBR[civil::weekday(days) as usize]),
            'A' => out.push_str(DAYS_FULL[civil::weekday(days) as usize]),
            'b' | 'h' => out.push_str(MONTHS_ABBR[(month - 1) as usize]),
            'B' => out.push_str(MONTHS_FULL[(month - 1) as usize]),
            'c' => out.push_str(&format!(
                "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
            )),
            'd' => out.push_str(&format!("{day:02}")),
            'f' => out.push_str(&format!("{micros:06}")),
            'G' => out.push_str(&format!("{:04}", civil::iso_week(days).0)),
            'H' => out.push_str(&format!("{hour:02}")),
            'I' => out.push_str(&format!(
                "{:02}",
                if hour % 12 == 0 { 12 } else { hour % 12 }
            )),
            'j' => out.push_str(&format!("{:03}", civil::day_of_year(days))),
            'm' => out.push_str(&format!("{month:02}")),
            'M' => out.push_str(&format!("{minute:02}")),
            'p' => out.push_str(if hour < 12 { "AM" } else { "PM" }),
            'S' => out.push_str(&format!("{second:02}")),
            'T' | 'X' => out.push_str(&format!("{hour:02}:{minute:02}:{second:02}")),
            'u' => out.push_str(&format!("{}", civil::iso_weekday(days))),
            'U' => out.push_str(&format!("{:02}", civil::week_of_year_sunday(days))),
            'V' => out.push_str(&format!("{:02}", civil::iso_week(days).1)),
            'w' => out.push_str(&format!("{}", civil::weekday(days))),
            'W' => out.push_str(&format!("{:02}", civil::week_of_year_monday(days))),
            'x' => out.push_str(&format!("{year:04}-{month:02}-{day:02}")),
            'y' => out.push_str(&format!("{:02}", year.rem_euclid(100))),
            'Y' => out.push_str(&format!("{year:04}")),
            '%' => out.push('%'),
            other => return Err(unsupported("strftime", other)),
        }
    }
    Ok(out)
}

// ---------- strptime ----------

/// The fields a format can set; everything starts at the pinned build's
/// defaults (1900-01-01 00:00:00).
#[derive(Debug, Clone, Copy)]
struct State {
    year: i32,
    month: i64,
    day: i64,
    hour24: Option<i64>,
    hour12: Option<i64>,
    pm: Option<bool>,
    minute: i64,
    second: i64,
    micros: i64,
    /// `%j`: 1-based day of year; drives the date when present.
    day_of_year: Option<i64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            year: 1900,
            month: 1,
            day: 1,
            hour24: None,
            hour12: None,
            pm: None,
            minute: 0,
            second: 0,
            micros: 0,
            day_of_year: None,
        }
    }
}

/// Parse `input` according to `format` and return microseconds since the
/// Unix epoch (a naive TIMESTAMP).
pub fn strptime(input: &str, format: &str) -> Result<i64, String> {
    if format.is_empty() {
        return Err("strptime: empty format string".to_string());
    }
    let bytes = input.as_bytes();
    let mut state = State::default();
    let mut pos = 0usize;
    parse_into(&mut state, bytes, &mut pos, format)?;
    if pos != bytes.len() {
        return Err(format!(
            "strptime: the input has {} trailing character(s) after the format",
            bytes.len() - pos
        ));
    }
    state.finish()
}

fn parse_into(
    state: &mut State,
    input: &[u8],
    pos: &mut usize,
    format: &str,
) -> Result<(), String> {
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).as_bytes() {
                if input.get(*pos) != Some(b) {
                    return Err(format!(
                        "strptime: literal {c:?} does not match the input at offset {}",
                        *pos
                    ));
                }
                *pos += 1;
            }
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err("strptime: the format ends with a trailing '%'".to_string());
        };
        match spec {
            'Y' => state.year = read_year(input, pos)?,
            'y' => {
                let v = read_number(input, pos, 2, "%y")?;
                state.year = if v <= 68 {
                    (2000 + v) as i32
                } else {
                    (1900 + v) as i32
                };
            }
            'm' => {
                state.month = read_ranged(input, pos, 2, "%m", 1..=12, "month")?;
            }
            'd' => {
                state.day = read_ranged(input, pos, 2, "%d", 1..=31, "day")?;
            }
            'H' => {
                state.hour24 = Some(read_ranged(input, pos, 2, "%H", 0..=23, "hour")?);
            }
            'I' => {
                state.hour12 = Some(read_ranged(input, pos, 2, "%I", 1..=12, "12-hour hour")?);
            }
            'M' => {
                state.minute = read_ranged(input, pos, 2, "%M", 0..=59, "minute")?;
            }
            'S' => {
                state.second = read_ranged(input, pos, 2, "%S", 0..=59, "second")?;
            }
            'f' => {
                let start = *pos;
                let v = read_number(input, pos, 6, "%f")?;
                let digits = (*pos - start) as u32;
                state.micros = v * 10i64.pow(6 - digits);
            }
            'j' => {
                state.day_of_year = Some(read_ranged(input, pos, 3, "%j", 1..=366, "day of year")?);
            }
            'p' => {
                let word = read_letters(input, pos, "%p")?;
                state.pm = Some(if word.eq_ignore_ascii_case("AM") {
                    false
                } else if word.eq_ignore_ascii_case("PM") {
                    true
                } else {
                    return Err(format!("strptime: %p expects AM or PM, got {word:?}"));
                });
            }
            'a' => {
                let word = read_letters(input, pos, "%a")?;
                if !DAYS_ABBR.iter().any(|d| d.eq_ignore_ascii_case(&word)) {
                    return Err(format!("strptime: %a expects a weekday name, got {word:?}"));
                }
            }
            'A' => {
                let word = read_letters(input, pos, "%A")?;
                if !DAYS_FULL.iter().any(|d| d.eq_ignore_ascii_case(&word)) {
                    return Err(format!(
                        "strptime: %A expects a full weekday name, got {word:?}"
                    ));
                }
            }
            'b' | 'h' => {
                let word = read_letters(input, pos, "%b")?;
                state.month = name_index(&MONTHS_ABBR, &word)
                    .map(|i| i as i64 + 1)
                    .ok_or_else(|| {
                        format!("strptime: %{spec} expects an abbreviated month name, got {word:?}")
                    })?;
            }
            'B' => {
                let word = read_letters(input, pos, "%B")?;
                state.month = name_index(&MONTHS_FULL, &word)
                    .map(|i| i as i64 + 1)
                    .ok_or_else(|| {
                        format!("strptime: %B expects a full month name, got {word:?}")
                    })?;
            }
            'u' => {
                read_ranged(input, pos, 1, "%u", 1..=7, "ISO weekday")?;
            }
            'w' => {
                read_ranged(input, pos, 1, "%w", 0..=6, "weekday")?;
            }
            // Composite specifiers expand to their pinned spellings, so
            // "2024-3-5 4:5:6" parses under %c exactly like DuckDB's.
            'c' => parse_into(state, input, pos, "%Y-%m-%d %H:%M:%S")?,
            'x' => parse_into(state, input, pos, "%Y-%m-%d")?,
            'X' | 'T' => parse_into(state, input, pos, "%H:%M:%S")?,
            '%' => {
                if input.get(*pos) != Some(&b'%') {
                    return Err(format!(
                        "strptime: literal % does not match the input at offset {}",
                        *pos
                    ));
                }
                *pos += 1;
            }
            other => return Err(unsupported("strptime", other)),
        }
    }
    Ok(())
}

impl State {
    fn finish(&self) -> Result<i64, String> {
        let days: i64 = match self.day_of_year {
            // %j is applied as "Jan 1 + (j - 1)": 366 in a non-leap year is
            // the next year's Jan 1, exactly like the pinned build.
            Some(j) => civil::days_from_civil(self.year, 1, 1) + (j - 1),
            None => {
                let d = civil::checked_date(self.year, self.month as i32, self.day as i32)
                    .ok_or_else(|| {
                        format!(
                            "strptime: date out of range: {}-{}-{}",
                            self.year, self.month, self.day
                        )
                    })?;
                d as i64
            }
        };
        let hour = match (self.hour12, self.pm) {
            // %I wins over %p alone; 12 AM is 00, 12 PM is 12.
            (Some(h12), pm) => h12 % 12 + if pm == Some(true) { 12 } else { 0 },
            (None, Some(true)) => 12,
            (None, _) => self.hour24.unwrap_or(0),
        };
        let micros = days * 86_400_000_000
            + hour * 3_600_000_000
            + self.minute * 60_000_000
            + self.second * 1_000_000
            + self.micros;
        Ok(micros)
    }
}

/// A signed 4-digit year (`%Y`); a leading `-` is accepted so the extension's
/// own `%Y` output round-trips for BC years.
fn read_year(input: &[u8], pos: &mut usize) -> Result<i32, String> {
    let negative = input.get(*pos) == Some(&b'-');
    if negative {
        *pos += 1;
    }
    let start = *pos;
    let mut value: i64 = 0;
    while *pos < input.len() && *pos - start < 4 && input[*pos].is_ascii_digit() {
        value = value * 10 + (input[*pos] - b'0') as i64;
        *pos += 1;
    }
    if *pos == start {
        return Err(format!(
            "strptime: %Y expects a year (up to 4 digits) at offset {start}"
        ));
    }
    Ok(if negative {
        -(value as i32)
    } else {
        value as i32
    })
}

/// A greedy digit run of 1..=`max` digits.
fn read_number(input: &[u8], pos: &mut usize, max: usize, spec: &str) -> Result<i64, String> {
    let start = *pos;
    let mut value: i64 = 0;
    while *pos < input.len() && *pos - start < max && input[*pos].is_ascii_digit() {
        value = value * 10 + (input[*pos] - b'0') as i64;
        *pos += 1;
    }
    if *pos == start {
        return Err(format!(
            "strptime: {spec} expects a number at offset {start}"
        ));
    }
    Ok(value)
}

/// A bounded numeric field; the range error matches the pinned build's
/// wording ("month out of range", ...).
fn read_ranged(
    input: &[u8],
    pos: &mut usize,
    max: usize,
    spec: &str,
    range: std::ops::RangeInclusive<i64>,
    field: &str,
) -> Result<i64, String> {
    let value = read_number(input, pos, max, spec)?;
    if !range.contains(&value) {
        return Err(format!(
            "strptime: {field} {value} out of range ({}-{})",
            range.start(),
            range.end()
        ));
    }
    Ok(value)
}

/// A run of ASCII letters, 1 character or more.
fn read_letters(input: &[u8], pos: &mut usize, spec: &str) -> Result<String, String> {
    let start = *pos;
    while *pos < input.len() && input[*pos].is_ascii_alphabetic() {
        *pos += 1;
    }
    if *pos == start {
        return Err(format!("strptime: {spec} expects a name at offset {start}"));
    }
    Ok(String::from_utf8_lossy(&input[start..*pos]).into_owned())
}

/// Case-insensitive exact match against a name table (abbreviations and full
/// names are distinct, exactly like the pinned build: `%b` rejects "March").
fn name_index(table: &[&str], word: &str) -> Option<usize> {
    table
        .iter()
        .position(|name| name.eq_ignore_ascii_case(word))
}
