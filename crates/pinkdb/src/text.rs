//! Terminal-safe text rendering for untrusted identifiers and values.
//!
//! SQL identifiers may legally contain any byte except `"` and NUL (a quoted
//! identifier), and a hostile server may send arbitrary column names over the
//! wire. The CLI therefore never prints such text raw: [`sanitize_terminal`]
//! escapes every character that a terminal could interpret as a control
//! sequence, while leaving ordinary UTF-8 (including non-ASCII letters)
//! untouched.
//!
//! The rules are deliberately the same ones the CLI has always applied to
//! VARCHAR cells:
//!
//! - `\n` and `\r` become a space (a raw newline would break the result box);
//! - a tab is kept (it is a normal cell value; alignment is computed on the
//!   sanitized text, so it still has one code point of width);
//! - every other C0/C1 control (notably ESC, 0x1B) is shown as `\u{1b}`;
//! - Unicode format characters that terminals may act on invisibly (bidi
//!   overrides/isolates, zero-width joiners/marks, the BOM) are escaped the
//!   same way.
//!
//! Because ESC is escaped, the introducer of every ANSI/CSI/OSC/DCS sequence
//! is neutralized; the rest of the sequence becomes ordinary printable text.

/// Escape control and invisible-format characters so untrusted text can be
/// printed to a terminal safely. Valid UTF-8 is preserved; only characters a
/// terminal could treat specially are rewritten.
pub fn sanitize_terminal(s: &str) -> String {
    if !s.chars().any(is_unsafe) {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\t' => out.push('\t'),
            '\n' | '\r' => out.push(' '),
            c if is_unsafe(c) => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// A character a terminal may interpret instead of displaying it literally.
fn is_unsafe(c: char) -> bool {
    c.is_control()
        || matches!(
            c as u32,
            // Zero-width space/joiners/marks, LRM/RLM and the Arabic letter
            // mark.
            0x061C | 0x200B..=0x200F
            // Bidi embeddings/overrides and isolates.
            | 0x202A..=0x202E
            | 0x2066..=0x2069
            // Invisible operators and deprecated format characters.
            | 0x2060..=0x2064
            | 0x206A..=0x206F
            // Byte-order mark and interlinear annotation controls.
            | 0xFEFF
            | 0xFFF9..=0xFFFB
        )
}

#[cfg(test)]
mod tests {
    use super::sanitize_terminal;

    #[test]
    fn leaves_legitimate_utf8_untouched() {
        assert_eq!(sanitize_terminal("Chloé"), "Chloé");
        assert_eq!(sanitize_terminal("naïve 日本語"), "naïve 日本語");
        assert_eq!(sanitize_terminal("plain"), "plain");
    }

    #[test]
    fn escapes_ansi_osc_and_c1_sequences() {
        let injected = "x\u{1b}[31mRED\u{1b}[0m";
        let safe = sanitize_terminal(injected);
        assert!(!safe.contains('\u{1b}'), "{safe:?}");
        assert_eq!(safe, "x\\u{1b}[31mRED\\u{1b}[0m");

        let osc = "\u{1b}]0;pwned\u{7}";
        let safe = sanitize_terminal(osc);
        assert!(
            !safe.contains('\u{1b}') && !safe.contains('\u{7}'),
            "{safe:?}"
        );

        // C1 CSI (0x9B) is a control too.
        let c1 = "a\u{9b}2Jb";
        assert_eq!(sanitize_terminal(c1), "a\\u{9b}2Jb");
    }

    #[test]
    fn keeps_tab_and_maps_newlines_to_space() {
        assert_eq!(sanitize_terminal("a\tb"), "a\tb");
        assert_eq!(sanitize_terminal("a\nb\rc"), "a b c");
    }

    #[test]
    fn escapes_bidi_and_zero_width_format_characters() {
        let spoof = "user\u{202e}gpj.exe";
        assert_eq!(sanitize_terminal(spoof), "user\\u{202e}gpj.exe");
        assert!(!sanitize_terminal("a\u{200b}b").contains('\u{200b}'));
    }
}
