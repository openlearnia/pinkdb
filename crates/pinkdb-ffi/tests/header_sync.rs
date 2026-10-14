//! `include/pinkdb.h` must stay in lockstep with the Rust side: every
//! `PINKDB_STMT_*` / `PINKDB_TYPE_*` enumerator equals the Rust constant of
//! the same name, and the set of `pinkdb_*` functions the header declares is
//! exactly the set `src/lib.rs` exports.

use std::collections::BTreeSet;

const HEADER: &str = include_str!("../include/pinkdb.h");
const SOURCE: &str = include_str!("../src/lib.rs");

/// `NAME = value` enumerators from the header.
fn header_enumerators() -> Vec<(String, i32)> {
    HEADER
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("PINKDB_STMT_") || l.starts_with("PINKDB_TYPE_"))
        .filter_map(|l| {
            let (name, value) = l.trim_end_matches(',').split_once('=')?;
            Some((name.trim().to_string(), value.trim().parse().ok()?))
        })
        .collect()
}

fn rust_const(name: &str) -> Option<i32> {
    let needle = format!("pub const {name}: i32 = ");
    let line = SOURCE
        .lines()
        .find(|l| l.trim_start().starts_with(&needle))?;
    line.trim_start()[needle.len()..]
        .trim_end_matches(';')
        .trim()
        .parse()
        .ok()
}

fn names_after(text: &str, marker: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|l| {
            let i = l.find(marker)? + marker.len() - "pinkdb_".len();
            let rest = &l[i..];
            let end = rest.find('(')?;
            Some(rest[..end].trim().to_string())
        })
        .filter(|n| n.starts_with("pinkdb_"))
        .collect()
}

#[test]
fn enumerators_match_the_rust_constants() {
    let enums = header_enumerators();
    assert!(enums.len() >= 20, "parsed too few enumerators: {enums:?}");
    for (name, value) in &enums {
        assert_eq!(
            rust_const(name),
            Some(*value),
            "{name} differs between pinkdb.h and lib.rs"
        );
    }
    // And no Rust constant is missing from the header.
    for line in SOURCE.lines() {
        let line = line.trim_start();
        if let Some(rest) = line.strip_prefix("pub const PINKDB_") {
            let name = format!("PINKDB_{}", rest.split(':').next().unwrap());
            if name.starts_with("PINKDB_STMT_") || name.starts_with("PINKDB_TYPE_") {
                assert!(
                    enums.iter().any(|(n, _)| *n == name),
                    "{name} missing from pinkdb.h"
                );
            }
        }
    }
}

#[test]
fn declared_functions_match_the_exports() {
    let declared: BTreeSet<String> = HEADER
        .lines()
        .filter(|l| !l.trim_start().starts_with('*') && !l.trim_start().starts_with("/*"))
        .filter_map(|l| {
            // The identifier directly before `(`: return types such as
            // `pinkdb_handle *` come first on the line.
            let end = l.find('(')?;
            let head = l[..end].trim_end();
            let start = head
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .map_or(0, |i| i + 1);
            let name = &head[start..];
            name.starts_with("pinkdb_").then(|| name.to_string())
        })
        .collect();
    let exported = names_after(SOURCE, "pub extern \"C\" fn pinkdb_");
    assert!(!exported.is_empty());
    assert_eq!(
        declared, exported,
        "pinkdb.h declarations vs lib.rs exports"
    );
}
