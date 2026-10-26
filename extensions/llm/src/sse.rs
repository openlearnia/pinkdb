//! A hand-rolled Server-Sent Events reader.
//!
//! `text/event-stream` is line-oriented: `field: value` lines, a blank line
//! dispatches the event, and `data:` fields concatenate with newlines. The
//! OpenAI-compatible chat stream uses nothing else, so this reads `data` and
//! ignores the other fields; the caller sees each payload as one string.
//!
//! The one rule that matters for an agent: a stream that ends without the
//! caller saying "done" is an *error*, not an empty answer. The run re-requests
//! that step instead of recording half a turn.

use std::io::BufRead;

/// A single `data:` line this large is not a chat chunk; refuse it instead of
/// growing a buffer until memory runs out.
const MAX_LINE_BYTES: u64 = 1 << 20;

/// Read `data:` payloads until `on_data` returns `Ok(false)` (the caller saw
/// its terminator) or the stream ends.
///
/// Returns an error when the stream ends first: a closed socket in the middle
/// of an event and a closed socket between events are both "interrupted".
pub fn read_events(
    reader: &mut impl BufRead,
    mut on_data: impl FnMut(&str) -> Result<bool, String>,
) -> Result<(), String> {
    let mut line = Vec::new();
    let mut data = String::new();
    let mut saw_field = false;
    loop {
        if read_line(reader, &mut line)? == 0 {
            return Err("the provider closed the stream before it finished".to_string());
        }
        let text = std::str::from_utf8(&line)
            .map_err(|_| "the provider sent a stream line that is not UTF-8".to_string())?;
        let text = text.trim_end_matches(['\n', '\r']);
        if text.is_empty() {
            if saw_field {
                saw_field = false;
                let payload = std::mem::take(&mut data);
                if !on_data(&payload)? {
                    return Ok(());
                }
            }
            continue;
        }
        if text.starts_with(':') {
            continue;
        }
        let Some((field, value)) = text.split_once(':') else {
            continue;
        };
        if field != "data" {
            continue;
        }
        let value = value.strip_prefix(' ').unwrap_or(value);
        if saw_field {
            data.push('\n');
        }
        data.push_str(value);
        saw_field = true;
    }
}

/// One line, capped: `BufRead::read_line` has no limit of its own, and a
/// provider that never sends a newline must not grow a buffer forever.
fn read_line<R: BufRead + ?Sized>(reader: &mut R, line: &mut Vec<u8>) -> Result<usize, String> {
    line.clear();
    loop {
        let (done, used) = {
            let available = reader
                .fill_buf()
                .map_err(|e| format!("the provider's stream failed: {e}"))?;
            if available.is_empty() {
                return Ok(line.len());
            }
            let used = match available.iter().position(|byte| *byte == b'\n') {
                Some(index) => index + 1,
                None => available.len(),
            };
            if line.len() + used > MAX_LINE_BYTES as usize {
                return Err("the provider sent an event line larger than 1 MiB".to_string());
            }
            line.extend_from_slice(&available[..used]);
            (available[used - 1] == b'\n', used)
        };
        reader.consume(used);
        if done {
            return Ok(line.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(fixture: &str) -> Result<Vec<String>, String> {
        let mut reader = std::io::BufReader::new(fixture.as_bytes());
        let mut seen = Vec::new();
        read_events(&mut reader, |data| {
            if data.trim() == "[DONE]" {
                return Ok(false);
            }
            seen.push(data.to_string());
            Ok(true)
        })?;
        Ok(seen)
    }

    #[test]
    fn reads_data_events_and_stops_at_done() {
        let seen = collect("event: x\n\ndata: one\n\ndata: two\n\ndata: [DONE]\n\ndata: three\n\n")
            .expect("a complete stream");
        assert_eq!(seen, ["one", "two"]);
    }

    #[test]
    fn multi_line_data_is_joined() {
        let seen = collect("data: a\ndata: b\n\ndata: [DONE]\n\n").expect("a complete stream");
        assert_eq!(seen, ["a\nb"]);
    }

    #[test]
    fn a_stream_that_ends_early_is_an_error() {
        let error = collect("data: half\n\ndata: par").expect_err("an interrupted stream");
        assert!(error.contains("closed the stream"), "{error}");
    }

    #[test]
    fn an_event_without_a_trailing_blank_line_is_an_error() {
        assert!(collect("data: one").is_err());
    }
}
