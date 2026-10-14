//! The repository catalog (`<base>/index.json`, format v1) and artifact
//! resolution: platform/ABI filtering and semver-ish version selection.
//!
//! The format is specified in `ext-repo/FORMAT.md`. Clients MUST refuse a
//! `format_version` other than 1, and must match their own platform tag
//! exactly. Entries are validated on parse: a catalog with a malformed or
//! path-unsafe entry is rejected as a whole (fail closed), before anything is
//! downloaded.

use std::cmp::Ordering;

/// The only repository format this client understands.
pub const FORMAT_VERSION: u64 = 1;
/// The only extension ABI this client accepts.
pub const ABI_VERSION: u64 = 1;

/// One artifact entry of the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub version: String,
    pub abi: u64,
    pub platform: String,
    pub file: String,
    pub sha256: String,
    pub description: Option<String>,
}

/// A parsed `index.json`.
#[derive(Debug, Clone)]
pub struct Catalog {
    pub entries: Vec<Entry>,
}

impl Catalog {
    /// Parse and validate a catalog document.
    pub fn parse(bytes: &[u8]) -> Result<Catalog, String> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|e| format!("invalid catalog JSON: {e}"))?;
        let format_version = value
            .get("format_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "catalog is missing the integer field \"format_version\"".to_string())?;
        if format_version != FORMAT_VERSION {
            return Err(format!(
                "unsupported repository format version {format_version} (expected {FORMAT_VERSION})"
            ));
        }
        let list = value
            .get("extensions")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "catalog is missing the \"extensions\" array".to_string())?;
        let mut entries = Vec::with_capacity(list.len());
        for (i, item) in list.iter().enumerate() {
            entries.push(parse_entry(item).map_err(|e| format!("catalog entry {i}: {e}"))?);
        }
        Ok(Catalog { entries })
    }
}

fn parse_entry(value: &serde_json::Value) -> Result<Entry, String> {
    let name = string_field(value, "name")?;
    let version = string_field(value, "version")?;
    let platform = string_field(value, "platform")?;
    let file = string_field(value, "file")?;
    let sha256 = string_field(value, "sha256")?;
    let abi = value
        .get("abi")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "missing or non-integer field \"abi\"".to_string())?;
    let description = match value.get("description") {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .ok_or_else(|| "field \"description\" is not a string".to_string())?
                .to_string(),
        ),
    };

    // Catalog fields become filesystem path components in the cache, so a
    // malformed (or malicious) repository must not be able to escape
    // `$PINKDB_HOME/extensions`.
    for (field, v) in [
        ("name", &name),
        ("version", &version),
        ("platform", &platform),
    ] {
        if !is_safe_component(v) {
            return Err(format!(
                "field \"{field}\" is not a safe path component: {v:?}"
            ));
        }
    }
    if !is_safe_relative_path(&file) {
        return Err(format!(
            "field \"file\" is not a safe repository-relative path: {file:?}"
        ));
    }
    if sha256.len() != 64 || !sha256.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("field \"sha256\" is not 64 hex digits: {sha256:?}"));
    }
    Ok(Entry {
        name,
        version,
        abi,
        platform,
        file,
        sha256: sha256.to_ascii_lowercase(),
        description,
    })
}

fn string_field(value: &serde_json::Value, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing or non-string field \"{key}\""))
}

fn is_safe_component(v: &str) -> bool {
    !v.is_empty() && v != "." && v != ".." && !v.contains(['/', '\\', '\0'])
}

fn is_safe_relative_path(v: &str) -> bool {
    !v.is_empty()
        && !v.starts_with('/')
        && !v.contains(['\\', '\0'])
        && v.split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// The result of resolving `name[@version]` in one catalog.
#[derive(Debug)]
pub enum Lookup<'a> {
    Found(&'a Entry),
    /// No matching artifact in this catalog; the reason is for the user.
    NoMatch(String),
}

/// Resolve `name` (optionally an exact `version`) for `platform`/`abi`.
///
/// With an exact version the first platform+ABI match wins. Without one, the
/// highest semver-ish version among the platform+ABI matches is chosen.
pub fn lookup<'a>(
    catalog: &'a Catalog,
    name: &str,
    version: Option<&str>,
    platform: &str,
    abi: u64,
) -> Lookup<'a> {
    let named: Vec<&Entry> = catalog.entries.iter().filter(|e| e.name == name).collect();
    if named.is_empty() {
        return Lookup::NoMatch(format!("no extension named {name:?}"));
    }
    let candidates: Vec<&Entry> = match version {
        Some(v) => {
            let filtered: Vec<&Entry> = named.iter().copied().filter(|e| e.version == v).collect();
            if filtered.is_empty() {
                let mut versions: Vec<&str> = named.iter().map(|e| e.version.as_str()).collect();
                versions.sort();
                versions.dedup();
                return Lookup::NoMatch(format!(
                    "no version {v} (available: {})",
                    versions.join(", ")
                ));
            }
            filtered
        }
        None => named,
    };
    let matching: Vec<&Entry> = candidates
        .iter()
        .copied()
        .filter(|e| e.platform == platform && e.abi == abi)
        .collect();
    if matching.is_empty() {
        return Lookup::NoMatch(describe_mismatch(name, version, &candidates, platform, abi));
    }
    match version {
        Some(_) => Lookup::Found(matching[0]),
        None => Lookup::Found(
            matching
                .into_iter()
                .max_by(|a, b| compare_versions(&a.version, &b.version))
                .expect("non-empty"),
        ),
    }
}

fn describe_mismatch(
    name: &str,
    version: Option<&str>,
    candidates: &[&Entry],
    platform: &str,
    abi: u64,
) -> String {
    let mut combos: Vec<String> = candidates
        .iter()
        .map(|e| format!("{} (ABI {})", e.platform, e.abi))
        .collect();
    combos.sort();
    combos.dedup();
    let version = version.map(|v| format!(" version {v}")).unwrap_or_default();
    format!(
        "no artifact for {name:?}{version} on platform {platform} with ABI {abi} (available: {})",
        combos.join(", ")
    )
}

/// Semver-ish ordering: numeric dotted parts (missing = 0), then a release
/// beats a pre-release, then pre-release strings compare lexically. Versions
/// that do not start with digits still order by whatever parses.
fn version_key(v: &str) -> (Vec<u64>, Option<String>) {
    let (core, pre) = match v.split_once('-') {
        Some((core, pre)) => (core, Some(pre.to_string())),
        None => (v, None),
    };
    let parts = core
        .split('.')
        .map(|p| p.parse::<u64>().unwrap_or(0))
        .collect();
    (parts, pre)
}

fn compare_versions(a: &str, b: &str) -> Ordering {
    let (a_parts, a_pre) = version_key(a);
    let (b_parts, b_pre) = version_key(b);
    let len = a_parts.len().max(b_parts.len());
    for i in 0..len {
        let x = a_parts.get(i).copied().unwrap_or(0);
        let y = b_parts.get(i).copied().unwrap_or(0);
        match x.cmp(&y) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    match (a_pre, b_pre) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => a.cmp(&b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2";

    fn catalog(entries: &str) -> Catalog {
        let json = format!(
            r#"{{"format_version": 1, "updated": "2026-10-14T09:30:00Z", "extensions": [{entries}]}}"#
        );
        Catalog::parse(json.as_bytes()).unwrap()
    }

    fn entry(name: &str, version: &str, platform: &str, abi: u64) -> String {
        format!(
            r#"{{"name": "{name}", "version": "{version}", "abi": {abi}, "platform": "{platform}",
                "file": "artifacts/{name}/{version}/{platform}/{name}.dylib", "sha256": "{SHA}",
                "description": "test"}}"#
        )
    }

    #[test]
    fn parses_a_format_v1_catalog() {
        let cat = catalog(&entry("math_ext", "0.1.0", "darwin-arm64", 1));
        assert_eq!(cat.entries.len(), 1);
        let e = &cat.entries[0];
        assert_eq!(e.name, "math_ext");
        assert_eq!(e.version, "0.1.0");
        assert_eq!(e.abi, 1);
        assert_eq!(e.platform, "darwin-arm64");
        assert_eq!(
            e.file,
            "artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib"
        );
        assert_eq!(e.sha256, SHA);
        assert_eq!(e.description.as_deref(), Some("test"));
    }

    #[test]
    fn refuses_other_format_versions() {
        let json = r#"{"format_version": 2, "extensions": []}"#;
        let err = Catalog::parse(json.as_bytes()).unwrap_err();
        assert_eq!(err, "unsupported repository format version 2 (expected 1)");
    }

    #[test]
    fn rejects_malformed_documents_and_entries() {
        for bad in [
            "not json",
            "[]",
            r#"{"extensions": []}"#,
            r#"{"format_version": 1}"#,
        ] {
            assert!(Catalog::parse(bad.as_bytes()).is_err(), "{bad:?}");
        }
        let cases = [
            // Missing field ("platform" absent).
            r#"{"name": "x", "version": "1", "abi": 1, "file": "f", "sha256": "0000000000000000000000000000000000000000000000000000000000000000"}"#
                .to_string(),
            // Short sha256.
            entry("x", "1.0.0", "darwin-arm64", 1).replace(SHA, "abcd"),
            // Unsafe file path.
            entry("x", "1.0.0", "darwin-arm64", 1).replace(
                "artifacts/x/1.0.0/darwin-arm64/x.dylib",
                "../escape.dylib",
            ),
            // Unsafe name.
            entry("x", "1.0.0", "darwin-arm64", 1).replace("\"name\": \"x\"", "\"name\": \"a/b\""),
            // abi as a string.
            entry("x", "1.0.0", "darwin-arm64", 1).replace("\"abi\": 1", "\"abi\": \"1\""),
        ];
        for bad in cases {
            let json = format!(r#"{{"format_version": 1, "extensions": [{bad}]}}"#);
            assert!(Catalog::parse(json.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn version_ordering_is_semver_ish() {
        assert_eq!(compare_versions("0.10.0", "0.2.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("2.0.0", "10.0.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "1.0.0-rc1"), Ordering::Greater);
        assert_eq!(compare_versions("1.0.0-rc1", "1.0.0-rc2"), Ordering::Less);
    }

    #[test]
    fn picks_the_highest_version_for_the_platform() {
        let cat = catalog(&format!(
            "{},{},{},{}",
            entry("math_ext", "0.1.0", "darwin-arm64", 1),
            entry("math_ext", "0.2.0", "darwin-arm64", 1),
            entry("math_ext", "0.10.0", "darwin-arm64", 1),
            entry("math_ext", "0.3.0", "linux-x64", 1),
        ));
        match lookup(&cat, "math_ext", None, "darwin-arm64", 1) {
            Lookup::Found(e) => assert_eq!(e.version, "0.10.0"),
            other => panic!("expected a match, got {other:?}"),
        }
    }

    #[test]
    fn exact_version_is_required_when_given() {
        let cat = catalog(&format!(
            "{},{}",
            entry("math_ext", "0.1.0", "darwin-arm64", 1),
            entry("math_ext", "0.2.0", "darwin-arm64", 1),
        ));
        match lookup(&cat, "math_ext", Some("0.1.0"), "darwin-arm64", 1) {
            Lookup::Found(e) => assert_eq!(e.version, "0.1.0"),
            other => panic!("expected a match, got {other:?}"),
        }
        match lookup(&cat, "math_ext", Some("0.9.9"), "darwin-arm64", 1) {
            Lookup::NoMatch(reason) => {
                assert!(reason.contains("no version 0.9.9"), "{reason}");
                assert!(reason.contains("0.1.0, 0.2.0"), "{reason}");
            }
            other => panic!("expected no match, got {other:?}"),
        }
    }

    #[test]
    fn platform_and_abi_mismatches_say_what_is_available() {
        let cat = catalog(&format!(
            "{},{}",
            entry("math_ext", "0.1.0", "linux-x64", 1),
            entry("math_ext", "0.2.0", "darwin-arm64", 2),
        ));
        match lookup(&cat, "math_ext", None, "darwin-arm64", 1) {
            Lookup::NoMatch(reason) => {
                assert!(
                    reason.contains(
                        "no artifact for \"math_ext\" on platform darwin-arm64 with ABI 1"
                    ),
                    "{reason}"
                );
                assert!(reason.contains("linux-x64 (ABI 1)"), "{reason}");
                assert!(reason.contains("darwin-arm64 (ABI 2)"), "{reason}");
            }
            other => panic!("expected no match, got {other:?}"),
        }
        match lookup(&cat, "nope", None, "darwin-arm64", 1) {
            Lookup::NoMatch(reason) => {
                assert!(reason.contains("no extension named \"nope\""), "{reason}")
            }
            other => panic!("expected no match, got {other:?}"),
        }
    }
}
