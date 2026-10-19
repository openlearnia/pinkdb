//! File resolution: DuckDB's parquet path semantics, pinned against
//! `duckdb` v2.0.0-alpha46057.
//!
//! One path argument names one of:
//!
//! - a **literal file** (`fx/u.parquet`) — used as given;
//! - a **directory** (`fx/part`, `fx/part/`) — every `*.parquet` file under it,
//!   recursively (DuckDB's rule: the directory form filters by extension, a
//!   glob does not — α reads `fx/mixed/*` including `readme.txt` and fails on
//!   it, and `read_parquet('fx/mixed')` reads only the parquet file);
//! - a **glob** — `*` and `?` match within one path component (never `/`),
//!   `[abc]` / `[a-z]` / `[!abc]` are character classes, and `**` matches any
//!   number of directory levels (including none).
//!
//! Matches are **sorted lexicographically by path** — DuckDB's order, which
//! decides row order and, without `union_by_name`, which file's schema is the
//! reference. A pattern that matches nothing is an empty list; the caller
//! turns that into α's `No files found that match the pattern "…"` error
//! (or, with `allow_empty`, an empty result).

use std::path::Path;

/// True when `component` contains a glob metacharacter.
fn has_meta(component: &str) -> bool {
    component.bytes().any(|b| matches!(b, b'*' | b'?' | b'['))
}

/// Split a path into components, dropping `` and `.` (DuckDB's
/// `./fx` and `fx` name the same files).
fn components(path: &str) -> (bool, Vec<String>) {
    let absolute = path.starts_with('/');
    let parts: Vec<String> = path
        .split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .map(str::to_string)
        .collect();
    (absolute, parts)
}

fn join(base: &str, part: &str) -> String {
    if base.is_empty() {
        part.to_string()
    } else if base.ends_with('/') {
        format!("{base}{part}")
    } else {
        format!("{base}/{part}")
    }
}

/// Expand one path pattern into the matched file paths, sorted and deduped.
///
/// `Err` only for a real filesystem failure (a permission error while
/// walking, say); "nothing matched" is `Ok(vec![])`.
pub fn expand(pattern: &str) -> std::io::Result<Vec<String>> {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let path = Path::new(trimmed);
    // A literal path: a directory expands recursively to its parquet files.
    if !has_meta(trimmed) {
        let meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        if meta.is_dir() {
            let mut out = Vec::new();
            collect_dir(trimmed, &mut out)?;
            out.sort();
            out.dedup();
            return Ok(out);
        }
        return Ok(vec![trimmed.to_string()]);
    }

    let (absolute, comps) = components(trimmed);
    if comps.is_empty() {
        return Ok(Vec::new());
    }
    // The longest leading run of literal components is the walk's root.
    let mut base = if absolute {
        "/".to_string()
    } else {
        String::new()
    };
    let mut idx = 0;
    while idx < comps.len() && !has_meta(&comps[idx]) {
        base = join(&base, &comps[idx]);
        idx += 1;
    }
    if idx == comps.len() {
        // A pattern made of literal components joined by separators only.
        return Ok(if Path::new(&base).exists() {
            vec![base]
        } else {
            Vec::new()
        });
    }
    let root = if base.is_empty() {
        ".".to_string()
    } else {
        base
    };
    let mut out = Vec::new();
    walk(&root, &comps[idx..], &mut out)?;
    out.sort();
    out.dedup();
    Ok(out)
}

/// Recursively collect every file under `dir` (the trailing `**` form).
fn collect_files(dir: &str, out: &mut Vec<String>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let child = join(dir, &name);
        if entry.file_type()?.is_dir() {
            collect_files(&child, out)?;
        } else {
            out.push(child);
        }
    }
    Ok(())
}

/// Recursively collect `*.parquet` files under `dir` (the directory form).
fn collect_dir(dir: &str, out: &mut Vec<String>) -> std::io::Result<()> {
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let child = join(dir, &name);
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_dir(&child, out)?;
        } else if name.to_ascii_lowercase().ends_with(".parquet") {
            out.push(child);
        }
    }
    Ok(())
}

/// Match the remaining pattern components against `dir`'s entries.
fn walk(dir: &str, comps: &[String], out: &mut Vec<String>) -> std::io::Result<()> {
    let Some(first) = comps.first() else {
        return Ok(());
    };
    let rest = &comps[1..];
    if first == "**" {
        // `**` matches zero or more directory levels. Trailing (rest empty):
        // every file under `dir`, recursively — α's `read_parquet('dir/**')`.
        // Otherwise: try the rest right here (zero levels) and then one level
        // deeper (one or more levels).
        if rest.is_empty() {
            collect_files(dir, out)?;
            return Ok(());
        }
        walk(dir, rest, out)?;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let child = join(dir, &entry.file_name().to_string_lossy());
                    walk(&child, comps, out)?;
                }
            }
        }
        return Ok(());
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !match_component(first, &name) {
            continue;
        }
        let child = join(dir, &name);
        let file_type = entry.file_type()?;
        if rest.is_empty() {
            if file_type.is_file() {
                out.push(child);
            }
        } else if file_type.is_dir() {
            walk(&child, rest, out)?;
        }
    }
    Ok(())
}

/// Match one path component against a glob component (`*`, `?`, `[…]`).
fn match_component(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    match_from(&p, &n)
}

fn match_from(p: &[char], n: &[char]) -> bool {
    if p.is_empty() {
        return n.is_empty();
    }
    match p[0] {
        '*' => {
            // Try every split point; `*` never crosses into a shorter name.
            (0..=n.len()).any(|i| match_from(&p[1..], &n[i..]))
        }
        '?' => !n.is_empty() && match_from(&p[1..], &n[1..]),
        '[' => {
            let Some(close) = p.iter().position(|c| *c == ']') else {
                // An unterminated class is a literal `[` (like DuckDB's glob).
                return !n.is_empty() && n[0] == '[' && match_from(&p[1..], &n[1..]);
            };
            let class = &p[1..close];
            let (negated, class) = match class.first() {
                Some('!') => (true, &class[1..]),
                _ => (false, class),
            };
            let Some(&c) = n.first() else {
                return false;
            };
            let mut hit = false;
            let mut i = 0;
            while i < class.len() {
                if i + 2 < class.len() && class[i + 1] == '-' {
                    if class[i] <= c && c <= class[i + 2] {
                        hit = true;
                    }
                    i += 3;
                } else {
                    if class[i] == c {
                        hit = true;
                    }
                    i += 1;
                }
            }
            hit != negated && match_from(&p[close + 1..], &n[1..])
        }
        c => !n.is_empty() && n[0] == c && match_from(&p[1..], &n[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components_match_like_a_glob() {
        assert!(match_component("*.parquet", "u.parquet"));
        assert!(!match_component("*.parquet", "u.csv"));
        assert!(match_component("u?.parquet", "u1.parquet"));
        assert!(!match_component("u?.parquet", "u12.parquet"));
        assert!(match_component("region=*", "region=us"));
        assert!(match_component("[eu]s", "us"));
        assert!(match_component("[a-z]s", "us"));
        assert!(!match_component("[!a-z]s", "us"));
        assert!(match_component("[!0-9]s", "us"));
        assert!(match_component("*", "anything"));
        assert!(match_component("a*b*c", "aXXbYYc"));
        assert!(!match_component("a*b*c", "aXXbYY"));
        assert!(match_component("*[.]parquet", "u.parquet"));
    }

    #[test]
    fn expands_files_directories_and_globs() {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-parquet-glob-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let nested = dir.join("part/region=us");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(dir.join("part/region=eu")).unwrap();
        std::fs::write(dir.join("part/region=us/u.parquet"), b"x").unwrap();
        std::fs::write(dir.join("part/region=eu/e.parquet"), b"x").unwrap();
        std::fs::write(dir.join("part/notes.txt"), b"x").unwrap();
        std::fs::create_dir_all(dir.join("deep/a/b")).unwrap();
        std::fs::write(dir.join("deep/a/b/z.parquet"), b"x").unwrap();
        let base = dir.to_str().unwrap();

        let literal = expand(&format!("{base}/part/region=us/u.parquet")).unwrap();
        assert_eq!(literal.len(), 1);

        // A directory expands recursively to its parquet files only.
        let directory = expand(&format!("{base}/part")).unwrap();
        assert_eq!(directory.len(), 2);
        assert!(
            directory[0].ends_with("region=eu/e.parquet"),
            "{directory:?}"
        );
        assert!(
            directory[1].ends_with("region=us/u.parquet"),
            "{directory:?}"
        );

        // A glob reads every match (no extension filter — α's behavior: it
        // matches `notes.txt` too and fails on it later). `*` never crosses a
        // separator, so `part/*` names only the files directly under `part`.
        let glob = expand(&format!("{base}/part/region=*/u.parquet")).unwrap();
        assert_eq!(glob.len(), 1);
        let all = expand(&format!("{base}/part/*")).unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].ends_with("part/notes.txt"), "{all:?}");
        let two = expand(&format!("{base}/part/*/*.parquet")).unwrap();
        assert_eq!(two.len(), 2);

        let recursive = expand(&format!("{base}/deep/**/*.parquet")).unwrap();
        assert_eq!(recursive.len(), 1);
        assert!(recursive[0].ends_with("z.parquet"));

        let star_star = expand(&format!("{base}/**/z.parquet")).unwrap();
        assert_eq!(star_star.len(), 1);
        let star_star_files = expand(&format!("{base}/**")).unwrap();
        assert!(
            star_star_files.iter().any(|p| p.ends_with("z.parquet")),
            "{star_star_files:?}"
        );
        assert!(
            star_star_files.iter().any(|p| p.ends_with("notes.txt")),
            "{star_star_files:?}"
        );

        let dotted = expand(&format!("{base}/./part/**/*.parquet")).unwrap();
        assert_eq!(dotted.len(), 2);

        assert!(expand(&format!("{base}/nope*.parquet")).unwrap().is_empty());
        assert!(expand(&format!("{base}/nope.parquet")).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
