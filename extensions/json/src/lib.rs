//! pinkdb's core **json** extension (wave 52): the six DuckDB JSON functions
//! over JSON text carried in `VARCHAR`.
//!
//! v1 has no JSON type, so — like the C ABI's value model — JSON *is* text:
//! every function takes and returns `VARCHAR`, a JSON string stays a quoted
//! JSON string (`json_extract('{"a":"x"}', '$.a')` → `"x"`, quotes included),
//! and there is no `->`/`->>` operator, no implicit casting, and no
//! `json_extract_string`. The divergences are listed in full below.
//!
//! # The functions
//!
//! | function | arguments | returns |
//! |---|---|---|
//! | `json_extract(json, path)` | VARCHAR, VARCHAR | VARCHAR: the JSON *text* at `path` |
//! | `json_value(json, path)` | VARCHAR, VARCHAR | VARCHAR: the text of a scalar, SQL NULL for arrays/objects |
//! | `json_valid(json)` | VARCHAR | BOOLEAN |
//! | `json_array_length(json[, path])` | VARCHAR[, VARCHAR] | BIGINT |
//! | `json_object(k, v, …)` | VARCHARs | VARCHAR |
//! | `json_array(v, …)` | VARCHARs | VARCHAR |
//!
//! Semantics are pinned to DuckDB α46057, probed case by case (and covered by
//! `bench/duckdiff.py`'s json probes):
//!
//! * **NULL in, NULL out**: `json_extract(NULL, p)`, `json_extract(j, NULL)`,
//!   `json_array_length(NULL)`, `json_array_length(j, NULL)` are all NULL.
//! * **Missing path → NULL**: a key that is absent, an index out of range, or
//!   a step applied to the wrong value kind (`.a` on an array, `[0]` on a
//!   scalar) is NULL, never an error.
//! * **Invalid JSON → error**, except `json_valid`, which returns false
//!   (`json_array_length('nope')` errors, `json_valid('nope')` is false).
//! * `json_value` returns the JSON *text* of a scalar (numbers canonically
//!   re-rendered: `1.50` → `1.5`, `1e2` → `100.0`) and NULL for a JSON array,
//!   object, or `null` — matching this DuckDB build, where `json_value`
//!   returns VARCHAR (DuckDB's `json_extract` returns its JSON type; only the
//!   type differs, the text is identical).
//! * `json_array_length` is 0 for a value that is not an array
//!   (`json_array_length('{}')` → 0), `NULL` when the path misses, and
//!   errors on invalid JSON.
//! * `json_object` requires an even number of arguments (an execution-time
//!   check, where DuckDB refuses at bind time) and errors on a NULL key
//!   ("JSON key cannot be NULL"); a NULL value becomes JSON `null`. Both
//!   builders re-render their arguments as JSON **strings** — DuckDB's
//!   behavior for `VARCHAR` arguments — because v1 has no typed arguments to
//!   embed.
//! * JSON objects keep source order and duplicates (`{"a":1,"a":2}`), a path
//!   lookup returning the first match, and numbers outside `i64`/`u64`/`f64`
//!   (and the lenient `NaN`/`Infinity` literals) round-trip verbatim.
//!
//! # Divergences from DuckDB (v1, by design)
//!
//! * **No JSON type**: results are VARCHAR, DuckDB's are JSON (textually
//!   identical for every probe); `json_extract(...)::VARCHAR` is implicit
//!   here. There is no `->` / `->>` operator, no `json_extract_string`.
//! * **Typed arguments are refused, not cast**: ABI v1 is strictly typed, so
//!   `json_array(1)` is a type error in pinkdb ("use CAST") while DuckDB
//!   embeds the integer; write `json_array(CAST(1 AS VARCHAR))` for a JSON
//!   string, and note `json_array('1')` → `["1"]` in both engines.
//! * **No JSON-typed composition of builders**:
//!   `json_array(json_object('a', '1'))` binds — both sides are VARCHAR — but
//!   embeds the inner call as a JSON *string* (`["{\"a\":\"1\"}"]`), whereas
//!   DuckDB inlines the object (`[{"a":"1"}]`). A JSON-typed ABI value is the
//!   recorded follow-up.
//! * **No wildcards**: `[*]`, `[0,1]`, and `$..a` are refused with a clear
//!   "not supported in v1" error instead of DuckDB's results.
//! * **Lone surrogates**: `"\uD800"` is invalid (`json_valid` false, the
//!   extracting functions error), exactly like DuckDB.
//! * **Depth**: nesting deeper than 1000 levels is refused (DuckDB's parser
//!   is iterative); documents that deep are outside the v1 envelope.
//!
//! # Activation
//!
//! The extension is published to the process-global **linked registry**
//! (`pinkdb_ext_loader::linked`), the same model `LOAD EXTENSION '<name>'`
//! resolves on wasm and as a native fallback:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! pinkdb_json::ensure_registered()?;              // host bootstrap, once
//! let mut db = pinkdb::PinkDb::new();
//! db.load_linked_extension("json")?;              // or: LOAD EXTENSION 'json'
//! # Ok(())
//! # }
//! ```
//!
//! The CLI's default `json` feature does both at startup — it publishes the
//! extension and loads it into every store the session creates or opens, so
//! `json_*` works without a statement (DuckDB's behavior; `LOAD EXTENSION
//! 'json'` is then redundant and reports the usual duplicate-load error).
//! `--no-default-features` leaves it out. A durable store replays JSON calls
//! through the registry, so the extension must be registered *before* a
//! database is opened, exactly like every linked extension. All six functions
//! are pure — no volatile flag, no WAL caveat.
//!
//! The crate links **no dependencies** beyond `pinkdb-ext-loader` (JSON
//! parsing and rendering are hand rolled, a few KB of code, so the default
//! binary does not pull in `serde_json` and friends).

mod parse;
mod path;
mod value;

pub use parse::MAX_DEPTH;
pub use path::{Path, Step};
pub use value::Json;

use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtError, ExtType, ExtValue};

/// The registry name: `LOAD EXTENSION 'json'`.
pub const EXTENSION_NAME: &str = "json";

// ---------- the six functions (pure, unit-testable) ----------

/// `json_extract(json, path)`: the JSON text at `path`, or NULL when the
/// path does not resolve. Errors on malformed JSON or a malformed path.
pub fn json_extract(json: Option<&str>, path: Option<&str>) -> Result<Option<String>, String> {
    let (Some(json), Some(path)) = (json, path) else {
        return Ok(None);
    };
    let doc = parse::parse(json)?;
    let path = path::parse_path(path)?;
    Ok(path::lookup(&doc, &path).map(value::to_string))
}

/// `json_value(json, path)`: like [`json_extract`], but a JSON array, object,
/// or `null` yields SQL NULL — the scalar view of the same extraction.
pub fn json_value(json: Option<&str>, path: Option<&str>) -> Result<Option<String>, String> {
    let (Some(json), Some(path)) = (json, path) else {
        return Ok(None);
    };
    let doc = parse::parse(json)?;
    let path = path::parse_path(path)?;
    let Some(value) = path::lookup(&doc, &path) else {
        return Ok(None);
    };
    Ok(match value {
        Json::Null | Json::Array(_) | Json::Object(_) => None,
        scalar => Some(value::to_string(scalar)),
    })
}

/// `json_valid(json)`: is `json` a well-formed document? NULL propagates.
pub fn json_valid(json: Option<&str>) -> Option<bool> {
    json.map(parse::is_valid)
}

/// `json_array_length(json[, path])`: the element count of the array at
/// `path` (the document itself when no path is given). A value that is not an
/// array is 0; a path that does not resolve is NULL; malformed JSON is an
/// error. A NULL path is NULL (the caller handles it before calling).
pub fn json_array_length(json: Option<&str>, path: Option<&str>) -> Result<Option<i64>, String> {
    let Some(json) = json else {
        return Ok(None);
    };
    let doc = parse::parse(json)?;
    let value = match path {
        Some(path) => {
            let path = path::parse_path(path)?;
            match path::lookup(&doc, &path) {
                Some(value) => value,
                None => return Ok(None),
            }
        }
        None => &doc,
    };
    Ok(Some(value.array_len().unwrap_or(0) as i64))
}

/// `json_object(k, v, …)`: an object from alternating keys and values, in
/// order (duplicate keys are kept, like DuckDB). `json_object()` is `{}`.
///
/// v1 takes VARCHAR arguments, so every value is embedded as a JSON
/// **string** — exactly what DuckDB does for VARCHAR arguments. NULL values
/// become JSON `null`; a NULL key is an error ("JSON key cannot be NULL"); an
/// odd argument count is an error ("requires an even number of arguments":
/// execution-time here, DuckDB refuses it at bind time — both error).
pub fn json_object(args: &[Option<&str>]) -> Result<String, String> {
    if !args.len().is_multiple_of(2) {
        return Err("json_object() requires an even number of arguments".to_string());
    }
    let mut entries = Vec::with_capacity(args.len() / 2);
    for pair in args.chunks(2) {
        let key = pair[0].ok_or_else(|| "JSON key cannot be NULL".to_string())?;
        let value = match pair[1] {
            Some(text) => Json::Str(text.to_string()),
            None => Json::Null,
        };
        entries.push((key.to_string(), value));
    }
    Ok(value::to_string(&Json::Object(entries)))
}

/// `json_array(v, …)`: an array of the arguments, in order, each embedded as a
/// JSON **string** (see [`json_object`]); NULL becomes JSON `null`.
/// `json_array()` is `[]`.
pub fn json_array(args: &[Option<&str>]) -> Result<String, String> {
    let items = args
        .iter()
        .map(|arg| match arg {
            Some(text) => Json::Str(text.to_string()),
            None => Json::Null,
        })
        .collect();
    Ok(value::to_string(&Json::Array(items)))
}

// ---------- the linked extension ----------

/// The extension record: the six functions, ready for
/// `linked::register` / `PinkDb::load_linked_extension`.
///
/// Arity: `json_extract`, `json_value`, and `json_valid` are fixed;
/// `json_array_length` is *optional* (1 or 2 arguments) and `json_object` /
/// `json_array` are *variadic* (any number of arguments; `json_object` checks
/// that the count is even). All six declare `VARCHAR` arguments and return
/// `VARCHAR` except `json_valid` (BOOLEAN) and `json_array_length` (BIGINT).
pub fn extension() -> Result<LinkedExtension, ExtError> {
    use ExtType::{Bigint, Boolean, Varchar};
    LinkedExtension::new(
        EXTENSION_NAME,
        vec![
            LinkedFunction::new("json_extract", vec![Varchar, Varchar], Varchar, ext_extract)?,
            LinkedFunction::new("json_value", vec![Varchar, Varchar], Varchar, ext_value)?,
            LinkedFunction::new("json_valid", vec![Varchar], Boolean, ext_valid)?,
            LinkedFunction::new_optional(
                "json_array_length",
                vec![Varchar, Varchar],
                1,
                Bigint,
                ext_length,
            )?,
            LinkedFunction::new_variadic(
                "json_object",
                vec![Varchar, Varchar],
                0,
                Varchar,
                ext_object,
            )?,
            LinkedFunction::new_variadic("json_array", vec![Varchar], 0, Varchar, ext_array)?,
        ],
    )
}

/// Publish the extension in the process-global linked registry. Strict:
/// fails if the name is already taken, whatever published it.
pub fn register() -> Result<(), ExtError> {
    linked::register(extension()?)
}

/// Publish the extension unless it is already published *with these exact
/// functions* — the idempotent bootstrap for hosts (and tests) that may run
/// more than once in one process. A name owned by a different extension is
/// still an error.
pub fn ensure_registered() -> Result<(), ExtError> {
    match register() {
        Ok(()) => Ok(()),
        Err(error) => match linked::get(EXTENSION_NAME) {
            Some(existing) if same_shape(&existing) => Ok(()),
            _ => Err(error),
        },
    }
}

/// Whether `existing` declares exactly our function names (used to tell a
/// second bootstrap of *this* extension from a foreign owner of the name).
fn same_shape(existing: &LinkedExtension) -> bool {
    let Ok(ours) = extension() else {
        return false;
    };
    let mut a: Vec<&str> = ours.functions().iter().map(|f| f.name()).collect();
    let mut b: Vec<&str> = existing.functions().iter().map(|f| f.name()).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

// ---------- ABI marshalling ----------

/// A VARCHAR argument, or SQL NULL.
fn arg_str<'a>(arg: &'a ExtArg<'a>) -> Result<Option<&'a str>, String> {
    match arg {
        ExtArg::Varchar(bytes) => std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| "invalid UTF-8 VARCHAR argument".to_string()),
        ExtArg::Null(_) => Ok(None),
        other => Err(format!("unexpected argument {other:?}")),
    }
}

fn varchar_or_null(text: Option<String>) -> ExtValue {
    match text {
        Some(text) => ExtValue::Varchar(text.into_bytes()),
        None => ExtValue::Null,
    }
}

fn ext_extract(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let json = arg_str(&args[0])?;
    let path = arg_str(&args[1])?;
    Ok(varchar_or_null(json_extract(json, path)?))
}

fn ext_value(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let json = arg_str(&args[0])?;
    let path = arg_str(&args[1])?;
    Ok(varchar_or_null(json_value(json, path)?))
}

fn ext_valid(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let json = arg_str(&args[0])?;
    Ok(match json_valid(json) {
        Some(valid) => ExtValue::Boolean(valid),
        None => ExtValue::Null,
    })
}

fn ext_length(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let json = arg_str(&args[0])?;
    // A one-argument call has no path; a NULL path is NULL before any parse.
    let path = if args.len() == 2 {
        match arg_str(&args[1])? {
            Some(path) => Some(path),
            None => return Ok(ExtValue::Null),
        }
    } else {
        None
    };
    Ok(match json_array_length(json, path)? {
        Some(len) => ExtValue::Bigint(len),
        None => ExtValue::Null,
    })
}

fn ext_object(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(arg_str(arg)?);
    }
    Ok(ExtValue::Varchar(json_object(&values)?.into_bytes()))
}

fn ext_array(args: &[ExtArg<'_>]) -> Result<ExtValue, String> {
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(arg_str(arg)?);
    }
    Ok(ExtValue::Varchar(json_array(&values)?.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(doc: &str, path: &str) -> Option<String> {
        json_extract(Some(doc), Some(path)).unwrap()
    }

    /// Pinned against DuckDB α46057.
    #[test]
    fn extract_and_value_match_duckdb() {
        assert_eq!(ex(r#"{"a": 1.50}"#, "$.a").as_deref(), Some("1.5"));
        assert_eq!(ex(r#"{"a": 1e2}"#, "$.a").as_deref(), Some("100.0"));
        assert_eq!(
            ex(r#"{"a": {"b" : [1,  2] }}"#, "$.a").as_deref(),
            Some(r#"{"b":[1,2]}"#)
        );
        assert_eq!(
            ex(r#"{"a":"he said \"hi\""}"#, "$.a").as_deref(),
            Some(r#""he said \"hi\"""#)
        );
        assert_eq!(
            ex(r#"{"a":"\u0041\u00e9"}"#, "$.a").as_deref(),
            Some("\"Aé\"")
        );
        assert_eq!(ex(r#"{"a":1,"a":2}"#, "$.a").as_deref(), Some("1"));
        assert_eq!(ex("[10,20,30]", "$[1]").as_deref(), Some("20"));
        assert_eq!(
            ex(r#"{"a":[{"b":true}]}"#, "$.a[0].b").as_deref(),
            Some("true")
        );
        assert_eq!(ex(r#"{"a":1}"#, "$.missing"), None);
        assert_eq!(ex(r#"{"a":1}"#, "$[0]"), None);
        assert_eq!(ex(r#" {"a" : 1 } "#, "$").as_deref(), Some(r#"{"a":1}"#));
        assert_eq!(
            ex(r#"{"a": "\uD83D\uDE00"}"#, "$.a").as_deref(),
            Some("\"😀\"")
        );
        assert_eq!(ex(r#"{"a":1e400}"#, "$.a").as_deref(), Some("1e400"));
        assert_eq!(ex("null", "$").as_deref(), Some("null"));

        // json_value: scalars as text, compound values and JSON null as NULL.
        assert_eq!(
            json_value(Some(r#"{"a":1}"#), Some("$.a"))
                .unwrap()
                .as_deref(),
            Some("1")
        );
        assert_eq!(
            json_value(Some(r#"{"a":"x"}"#), Some("$.a"))
                .unwrap()
                .as_deref(),
            Some(r#""x""#)
        );
        assert_eq!(
            json_value(Some(r#"{"a":1.5}"#), Some("$.a"))
                .unwrap()
                .as_deref(),
            Some("1.5")
        );
        assert_eq!(
            json_value(Some(r#"{"a":true}"#), Some("$.a"))
                .unwrap()
                .as_deref(),
            Some("true")
        );
        for doc in [r#"{"a":null}"#, r#"{"a":[1]}"#, r#"{"a":{"b":1}}"#] {
            assert_eq!(json_value(Some(doc), Some("$.a")).unwrap(), None, "{doc}");
        }
        assert_eq!(
            json_value(Some(r#"{"a":1}"#), Some("$.missing")).unwrap(),
            None
        );
        assert_eq!(json_value(None, Some("$.a")).unwrap(), None);
        assert_eq!(json_extract(Some(r#"{"a":1}"#), None).unwrap(), None);
        assert_eq!(json_value(Some("null"), Some("$")).unwrap(), None);
    }

    #[test]
    fn nulls_and_errors() {
        assert_eq!(json_extract(None, Some("$.a")).unwrap(), None);
        assert_eq!(json_array_length(None, None).unwrap(), None);
        assert_eq!(json_array_length(Some("[]"), None).unwrap(), Some(0));
        assert!(
            json_array_length(Some("nope"), None)
                .unwrap_err()
                .contains("malformed JSON")
        );
        assert!(
            json_extract(Some("{"), Some("$"))
                .unwrap_err()
                .contains("malformed JSON")
        );
        assert!(
            json_extract(Some("{}"), Some("$.a."))
                .unwrap_err()
                .contains("JSON path error")
        );
        // A lone surrogate is invalid, like DuckDB.
        assert_eq!(json_valid(Some(r#""\uD800""#)), Some(false));
        assert!(json_extract(Some(r#""\uD800""#), Some("$")).is_err());
        assert_eq!(json_valid(None), None);
        assert_eq!(json_valid(Some("{")), Some(false));
    }

    #[test]
    fn length_matches_duckdb() {
        let len = |doc: &str, path: Option<&str>| json_array_length(Some(doc), path).unwrap();
        assert_eq!(len("[]", None), Some(0));
        assert_eq!(len("[1,2,3]", None), Some(3));
        assert_eq!(len(r#"{"a":[1,2]}"#, Some("$.a")), Some(2));
        assert_eq!(len("1", None), Some(0));
        assert_eq!(len(r#""abc""#, None), Some(0));
        assert_eq!(len(r#"{"a":1}"#, None), Some(0));
        assert_eq!(len("[]", Some("$.missing")), None);
        assert_eq!(len("1", Some("$.a")), None);
        assert_eq!(len(r#"{"a":1}"#, Some("$.a")), Some(0));
        assert_eq!(len("[[1],[2,3]]", Some("$[1]")), Some(2));
        assert_eq!(len("[]", Some("$[0]")), None);
    }

    #[test]
    fn builders_match_duckdb() {
        assert_eq!(json_object(&[]).unwrap(), "{}");
        assert_eq!(
            json_object(&[Some("a"), Some("1"), Some("b"), Some("x")]).unwrap(),
            r#"{"a":"1","b":"x"}"#
        );
        assert_eq!(json_object(&[Some("a"), None]).unwrap(), r#"{"a":null}"#);
        assert_eq!(
            json_object(&[Some("a"), Some("1"), Some("a"), Some("2")]).unwrap(),
            r#"{"a":"1","a":"2"}"#
        );
        // A VARCHAR value that looks like JSON stays a JSON string.
        assert_eq!(
            json_object(&[Some("a"), Some(r#"{"b":1}"#)]).unwrap(),
            r#"{"a":"{\"b\":1}"}"#
        );
        assert_eq!(
            json_object(&[Some("a")]).unwrap_err(),
            "json_object() requires an even number of arguments"
        );
        assert_eq!(
            json_object(&[None, Some("1")]).unwrap_err(),
            "JSON key cannot be NULL"
        );

        assert_eq!(json_array(&[]).unwrap(), "[]");
        assert_eq!(
            json_array(&[Some("1"), Some("2"), Some("3")]).unwrap(),
            r#"["1","2","3"]"#
        );
        assert_eq!(json_array(&[None]).unwrap(), "[null]");
        assert_eq!(json_array(&[Some("x")]).unwrap(), r#"["x"]"#);
    }

    #[test]
    fn extension_metadata_is_pinned() {
        let ext = extension().unwrap();
        assert_eq!(ext.name(), "json");
        let names: Vec<&str> = ext.functions().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "json_extract",
                "json_value",
                "json_valid",
                "json_array_length",
                "json_object",
                "json_array"
            ]
        );
        let by_name = |name: &str| {
            ext.functions()
                .iter()
                .find(|f| f.name() == name)
                .unwrap()
                .clone()
        };
        let f = by_name("json_extract");
        assert_eq!(f.min_args(), 2);
        assert_eq!(f.max_args(), 2);
        assert!(!f.is_variadic());
        assert_eq!(f.ret_type(), ExtType::Varchar);
        let f = by_name("json_array_length");
        assert_eq!((f.min_args(), f.max_args()), (1, 2));
        let f = by_name("json_object");
        assert_eq!((f.min_args(), f.max_args()), (0, 64));
        assert!(f.is_variadic());
        assert_eq!(f.arg_type_at(7), ExtType::Varchar);
        let f = by_name("json_array");
        assert_eq!(f.min_args(), 0);
        assert!(f.is_variadic());
        assert_eq!(by_name("json_valid").ret_type(), ExtType::Boolean);
        assert_eq!(by_name("json_array_length").ret_type(), ExtType::Bigint);
    }

    /// The callbacks behave exactly like the pure functions, including the
    /// error and NULL paths the executor takes.
    #[test]
    fn callbacks_marshal_arguments() {
        let ext = extension().unwrap();
        let call = |name: &str, args: &[ExtArg<'_>]| {
            ext.functions()
                .iter()
                .find(|f| f.name() == name)
                .unwrap()
                .call(args)
                .unwrap()
        };
        assert_eq!(
            call(
                "json_extract",
                &[ExtArg::Varchar(b"{\"a\":1}"), ExtArg::Varchar(b"$.a")]
            ),
            ExtValue::Varchar(b"1".to_vec())
        );
        assert_eq!(
            call(
                "json_extract",
                &[ExtArg::Null(ExtType::Varchar), ExtArg::Varchar(b"$.a")]
            ),
            ExtValue::Null
        );
        assert_eq!(
            call("json_valid", &[ExtArg::Varchar(b"{}")]),
            ExtValue::Boolean(true)
        );
        assert_eq!(
            call("json_valid", &[ExtArg::Null(ExtType::Varchar)]),
            ExtValue::Null
        );
        assert_eq!(
            call("json_array_length", &[ExtArg::Varchar(b"[1,2]")]),
            ExtValue::Bigint(2)
        );
        assert_eq!(
            call(
                "json_array_length",
                &[ExtArg::Varchar(b"[1,2]"), ExtArg::Null(ExtType::Varchar)]
            ),
            ExtValue::Null
        );
        assert_eq!(
            call(
                "json_array",
                &[
                    ExtArg::Varchar(b"a"),
                    ExtArg::Null(ExtType::Varchar),
                    ExtArg::Varchar(b"b")
                ]
            ),
            ExtValue::Varchar(br#"["a",null,"b"]"#.to_vec())
        );
        assert_eq!(
            call(
                "json_object",
                &[ExtArg::Varchar(b"k"), ExtArg::Varchar(b"v")]
            ),
            ExtValue::Varchar(br#"{"k":"v"}"#.to_vec())
        );
        let err = ext
            .functions()
            .iter()
            .find(|f| f.name() == "json_object")
            .unwrap()
            .call(&[ExtArg::Null(ExtType::Varchar), ExtArg::Varchar(b"v")])
            .unwrap_err();
        assert!(err.to_string().contains("JSON key cannot be NULL"), "{err}");
        // Arity violations are caught by the metadata checks, with the
        // ranged messages.
        let f = ext
            .functions()
            .iter()
            .find(|f| f.name() == "json_array_length")
            .unwrap();
        let err = f.call(&[]).unwrap_err().to_string();
        assert!(
            err.contains("expected between 1 and 2 argument(s), got 0"),
            "{err}"
        );
        // json_object() accepts 0 arguments ({}), json_array() the same ([]).
        let f = ext
            .functions()
            .iter()
            .find(|f| f.name() == "json_object")
            .unwrap();
        assert_eq!(f.call(&[]).unwrap(), ExtValue::Varchar(b"{}".to_vec()));
    }
}
