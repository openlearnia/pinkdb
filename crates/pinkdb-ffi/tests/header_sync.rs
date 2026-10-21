//! `include/pinkdb.h` must stay in lockstep with the Rust side: every
//! `PINKDB_STMT_*` / `PINKDB_TYPE_*` enumerator equals the Rust constant of
//! the same name, and the set of `pinkdb_*` functions the header declares is
//! exactly the set `src/lib.rs` exports.

use std::collections::BTreeSet;
use std::mem::{align_of, offset_of, size_of};

use pinkdb_ffi::PinkValue;

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

/// Fields of the `pink_value` typedef, in declaration order, with their C
/// type spelling.
fn pink_value_fields() -> Vec<(String, String)> {
    let start = HEADER
        .find("typedef struct pink_value {")
        .expect("pink_value typedef");
    let rest = &HEADER[start..];
    let end = rest.find("} pink_value;").expect("pink_value typedef end");
    rest[..end]
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty()
                || line.starts_with("/*")
                || line.starts_with('*')
                || line.ends_with('{')
            {
                return None;
            }
            let (ty, name) = line
                .trim_end_matches(';')
                .rsplit_once(char::is_whitespace)?;
            let name = name.trim().trim_start_matches('*').trim();
            let ty = ty.trim();
            let ty = if line.contains('*') {
                format!("{ty} *")
            } else {
                ty.to_string()
            };
            Some((name.to_string(), ty))
        })
        .collect()
}

/// `(size, align)` of the C spellings the struct may use on this target.
fn c_size_align(ty: &str, pointer: usize) -> (usize, usize) {
    match ty {
        "int32_t" => (4, 4),
        "int64_t" | "double" => (8, 8),
        "const char *" => (pointer, pointer),
        "size_t" => (pointer, pointer),
        other => panic!("pink_value has an unhandled C type: {other:?}"),
    }
}

/// Lay a `repr(C)` struct out from the header's field types.
fn c_layout(fields: &[(String, String)]) -> (Vec<usize>, usize) {
    let pointer = size_of::<usize>();
    let mut offsets = Vec::new();
    let mut offset = 0usize;
    let mut align = 1usize;
    for (_, ty) in fields {
        let (size, field_align) = c_size_align(ty, pointer);
        align = align.max(field_align);
        offset = offset.next_multiple_of(field_align);
        offsets.push(offset);
        offset += size;
    }
    (offsets, offset.next_multiple_of(align))
}

#[test]
fn pink_value_layout_matches_the_header() {
    let fields = pink_value_fields();
    let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "type_tag", "is_null", "int64", "double_", "text_ptr", "text_len"
        ],
        "pink_value's fields changed; update PinkValue and every binding"
    );
    let (offsets, size) = c_layout(&fields);

    let rust_names = [
        "type_tag", "is_null", "int64", "double_", "text_ptr", "text_len",
    ];
    let rust_offsets = [
        offset_of!(PinkValue, type_tag),
        offset_of!(PinkValue, is_null),
        offset_of!(PinkValue, int64),
        offset_of!(PinkValue, double),
        offset_of!(PinkValue, text_ptr),
        offset_of!(PinkValue, text_len),
    ];
    for (i, name) in rust_names.iter().enumerate() {
        assert_eq!(
            offsets[i], rust_offsets[i],
            "pink_value.{name}: header offset {} != Rust {}",
            offsets[i], rust_offsets[i]
        );
    }
    assert_eq!(
        size,
        size_of::<PinkValue>(),
        "pink_value size differs between the header and Rust"
    );
    assert_eq!(
        align_of::<PinkValue>(),
        size_of::<usize>(),
        "unexpected PinkValue alignment"
    );
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
