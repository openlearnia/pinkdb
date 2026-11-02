//! UUID values and the seeded mixer volatile functions draw from.
//!
//! A UUID is DuckDB's 16-byte type, held as one `u128` (the unsigned numeric
//! value DuckDB orders and groups by), printed in the canonical lowercase
//! `8-4-4-4-12` form. Input accepts what DuckDB accepts: optional braces, no
//! dashes, any case.
//!
//! # Why a seeded mixer
//!
//! pinkdb's WAL stores statement text, so `uuid()` and `random()` in a write
//! must produce the same values when the text replays. Each logged statement
//! therefore gets one 128-bit seed, recorded in its WAL record's memo and taken
//! back by the replay in the same order, and each call derives its value from
//! `(seed, nesting, call site, chunk, row)` with SplitMix64 rounds. DuckDB's own
//! `uuid()` draws from a PCG generator, not a CSPRNG (`GetRandomUUID` seeds a
//! `PCGSeededState` per chunk), so a statement seed makes pinkdb no weaker than
//! the reference: the values are random to a user who cannot predict the seed,
//! and identical on replay.

/// DuckDB's text form: lowercase, 8-4-4-4-12.
pub fn format(bits: u128) -> String {
    let hex = format!("{bits:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Parse a UUID text form, accepting what DuckDB does: optional surrounding
/// braces, dashes optional, any case, nothing else.
pub fn parse(text: &str) -> Option<u128> {
    // Braces are only accepted around the canonical dashed form, as DuckDB
    // rejects `{550e8400e29b41d4a716446655440000}`.
    let braced = text.starts_with('{');
    let body = text
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .unwrap_or(text);
    if braced && body.len() != 36 {
        return None;
    }
    let mut digits = [0u8; 32];
    let mut at = 0usize;
    for (i, c) in body.chars().enumerate() {
        // Dashes are only legal at the canonical positions, so a length-36
        // string is exactly `8-4-4-4-12`; a dash anywhere else leaves fewer
        // than 32 hex digits and fails below.
        if c == '-' && (i == 8 || i == 13 || i == 18 || i == 23) && body.len() == 36 {
            continue;
        }
        if at == 32 {
            return None;
        }
        digits[at] = c.to_digit(16)? as u8;
        at += 1;
    }
    if at != 32 {
        return None;
    }
    let hex: String = digits
        .iter()
        .map(|d| char::from(b"0123456789abcdef"[*d as usize]))
        .collect();
    u128::from_str_radix(&hex, 16).ok()
}

/// Force the version-4 and RFC 4122 variant bits, like DuckDB's `uuid()`.
pub fn version4(bits: u128) -> u128 {
    // Byte 6 holds the version (bits 79..76), byte 8 the variant (63..62:
    // the field is `u128` big-endian, so byte 0 is the most significant).
    let bits = (bits & !(0xfu128 << 76)) | (0x4u128 << 76);
    (bits & !(0x3u128 << 62)) | (0x2u128 << 62)
}

/// SplitMix64's finalizer.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// One call's 128 bits: every input reaches both halves before the final
/// rounds, so the low half (what `random()` reads) varies with the row too.
///
/// `seed` is the statement's own seed from its WAL record; `nest` is the
/// statement's depth, which keeps a trigger action's values apart from the
/// statement that fired it; `site` is the call's position in the bound
/// statement; `chunk` is the chunk's position in the operator's input and `row`
/// the row's position in the chunk, so parallel and serial execution agree.
pub fn mix(seed: u128, nest: u64, site: u32, chunk: u64, row: u64) -> u128 {
    let site = u64::from(site);
    let a = mix64(
        (seed as u64) ^ nest.rotate_left(41) ^ site.rotate_left(32) ^ chunk.rotate_left(17) ^ row,
    );
    let b = mix64(((seed >> 64) as u64) ^ nest ^ chunk ^ row.rotate_left(43) ^ a);
    let lo = mix64(a ^ b.rotate_left(29));
    let hi = mix64(b ^ lo.rotate_left(11));
    (u128::from(hi) << 64) | u128::from(lo)
}

/// `random()` in `[0, 1)`: 53 significant bits, the width a double holds
/// exactly, so the result is uniform over the representable range.
pub fn random_unit(bits: u128) -> f64 {
    ((bits as u64) >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_round_trip_and_input_forms() {
        let bits = 0x550e_8400_e29b_41d4_a716_4466_5544_0000u128;
        let text = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(format(bits), text);
        assert_eq!(parse(text), Some(bits));
        assert_eq!(parse("550E8400-E29B-41D4-A716-446655440000"), Some(bits));
        assert_eq!(parse("{550E8400-E29B-41D4-A716-446655440000}"), Some(bits));
        assert_eq!(parse("550E8400E29B41D4A716446655440000"), Some(bits));
        assert_eq!(parse("550e8400-e29b-41d4-a716-44665544000"), None);
        assert_eq!(parse("{550e8400e29b41d4a716446655440000"), None);
        assert_eq!(parse("550e8400-x29b-41d4-a716-446655440000"), None);
    }

    #[test]
    fn every_input_reaches_both_halves() {
        let rows: Vec<u128> = (0..1000).map(|row| mix(7, 0, 3, 0, row)).collect();
        let mut lo: Vec<u64> = rows.iter().map(|b| *b as u64).collect();
        let mut hi: Vec<u64> = rows.iter().map(|b| (*b >> 64) as u64).collect();
        lo.sort_unstable();
        lo.dedup();
        hi.sort_unstable();
        hi.dedup();
        assert_eq!(lo.len(), 1000, "the low half ignores the row");
        assert_eq!(hi.len(), 1000, "the high half ignores the row");
        let chunks: Vec<u128> = (0..8).map(|chunk| mix(7, 0, 3, chunk, 0)).collect();
        assert_eq!(chunks.len(), 8);
        assert!(chunks.windows(2).all(|w| w[0] != w[1]));
        assert!(rows.iter().all(|b| *b != mix(8, 0, 3, 0, 0)));
        assert!(rows.iter().all(|b| *b != mix(7, 0, 4, 0, 0)));
        assert!(rows.iter().all(|b| *b != mix(7, 1, 3, 0, 0)));
    }

    #[test]
    fn version_and_variant_bits_are_forced() {
        let bits = version4(mix(7, 0, 1, 0, 0));
        let text = format(bits);
        assert_eq!(&text[14..15], "4", "{text}");
        assert!(matches!(&text[19..20], "8" | "9" | "a" | "b"), "{text}");
    }

    #[test]
    fn the_mixer_is_deterministic_and_uses_every_input() {
        let seed = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210u128;
        let base = mix(seed, 0, 0, 0, 0);
        assert_eq!(base, mix(seed, 0, 0, 0, 0));
        for other in [
            mix(seed ^ 1, 0, 0, 0, 0),
            mix(seed, 0, 1, 0, 0),
            mix(seed, 0, 0, 1, 0),
            mix(seed, 0, 0, 0, 1),
        ] {
            assert_ne!(base, other);
        }
        let mut seen = std::collections::HashSet::new();
        for row in 0..10_000u64 {
            assert!(seen.insert(mix(seed, 0, 3, 0, row)), "row {row} repeated");
        }
    }

    #[test]
    fn random_unit_stays_in_range() {
        for i in 0..1000u64 {
            let x = random_unit(mix(9, 0, 2, 0, i));
            assert!((0.0..1.0).contains(&x), "{x}");
        }
    }
}
