//! End-to-end tests for `extensions/string` through a real engine, using the
//! **linked** loading model (the wave-49 registration this crate's
//! `pinkdb_extension!` declaration generates): registration, `LOAD EXTENSION
//! 'string'` by name, then SQL through the binder, optimizer and executor —
//! including WHERE/GROUP BY composition, prepared statements, the documented
//! error paths, and a durable WAL replay proof (every function is
//! deterministic, so a logged INSERT that calls them replays identically).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

/// Register the extension exactly once per process (the registry is
/// process-global and registration is one-shot).
fn register_once() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        string_ext::pinkdb_register_linked().expect("register the string extension");
    });
}

fn test_db() -> PinkDb {
    register_once();
    let mut db = PinkDb::new();
    let name = db.load_linked_extension("string").unwrap();
    assert_eq!(name, "string");
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err("statement should fail")
        .to_string()
}

fn text(value: &str) -> Value {
    Value::Varchar(value.to_string())
}

// ---------- loading and the declared surface ----------

#[test]
fn the_extension_loads_by_name_and_registers_the_duckdb_pinned_surface() {
    let mut db = test_db();
    // Every function is callable by its DuckDB name; `left`/`right` are
    // reserved keywords the parser relaxes only in call position.
    assert_eq!(
        rows(
            &mut db,
            "SELECT split_part('a,b,c', ',', CAST(2 AS BIGINT)), reverse('héllo'), \
             left('hello', CAST(2 AS BIGINT)), right('hello', CAST(-2 AS BIGINT)), \
             strpos('héllo', 'llo'), starts_with('hello', 'he'), ends_with('hello', 'x');"
        ),
        vec![vec![
            text("b"),
            text("olléh"),
            text("he"),
            text("llo"),
            Value::Bigint(3),
            Value::Boolean(true),
            Value::Boolean(false),
        ]]
    );
    // Unquoted keyword calls parse (and a duplicate load is still refused).
    assert_eq!(
        rows(&mut db, "SELECT left('héllo', CAST(2 AS BIGINT));"),
        vec![vec![text("hé")]]
    );
    let err = db.load_linked_extension("string").unwrap_err().to_string();
    assert!(err.contains("already"), "{err}");
}

#[test]
fn nulls_propagate_argument_by_argument() {
    let mut db = test_db();
    for sql in [
        "SELECT split_part(NULL, ',', CAST(1 AS BIGINT)) IS NULL;",
        "SELECT split_part('a,b', NULL, CAST(1 AS BIGINT)) IS NULL;",
        "SELECT split_part('a,b', ',', NULL) IS NULL;",
        "SELECT reverse(NULL) IS NULL;",
        "SELECT left(NULL, CAST(1 AS BIGINT)) IS NULL;",
        "SELECT left('x', NULL) IS NULL;",
        "SELECT right(NULL, CAST(1 AS BIGINT)) IS NULL;",
        "SELECT strpos(NULL, 'x') IS NULL;",
        "SELECT starts_with(NULL, 'x') IS NULL;",
        "SELECT ends_with('x', NULL) IS NULL;",
        "SELECT regexp_extract(NULL, 'a', 0, '') IS NULL;",
        "SELECT regexp_extract('a', NULL, 0, '') IS NULL;",
        "SELECT regexp_matches(NULL, 'a', '') IS NULL;",
        "SELECT regexp_matches('a', NULL, '') IS NULL;",
        "SELECT regexp_match(NULL, 'a') IS NULL;",
    ] {
        assert_eq!(
            rows(&mut db, sql),
            vec![vec![Value::Boolean(true)]],
            "{sql}"
        );
    }
    // The two DuckDB NULL quirks: a NULL group is '' (not NULL), and a NULL
    // options field is an error (not NULL).
    assert_eq!(
        rows(&mut db, "SELECT regexp_extract('abc', 'b', NULL, '') = '';"),
        vec![vec![Value::Boolean(true)]]
    );
    let err = error(&mut db, "SELECT regexp_matches('a', 'a', NULL);");
    assert!(err.contains("regex options must not be NULL"), "{err}");
    let err = error(&mut db, "SELECT regexp_extract('a', 'a', 0, NULL);");
    assert!(err.contains("regex options must not be NULL"), "{err}");
}

// ---------- regex semantics through the engine ----------

#[test]
fn regexp_functions_match_duckdb_semantics_and_errors() {
    let mut db = test_db();
    assert_eq!(
        rows(
            &mut db,
            "SELECT regexp_extract('abc-123', '([a-z]+)-([0-9]+)', 2, ''), \
                    regexp_extract('hello', 'z', 0, ''), \
                    regexp_extract('HELLO', 'hello', 0, 'i'), \
                    regexp_matches('hello', 'l', ''), \
                    regexp_matches('hello', '^e', ''), \
                    regexp_match('aa1b22', '[0-9]+');"
        ),
        vec![vec![
            text("123"),
            text(""),
            text("HELLO"),
            Value::Boolean(true),
            Value::Boolean(false),
            text("1"),
        ]]
    );
    // The `s` option flips `.`-vs-newline; `sm`/`ms` prove left-to-right
    // precedence (a real newline inside the literal, like DuckDB's `E'\n'`).
    assert_eq!(
        rows(
            &mut db,
            "SELECT regexp_matches('a\nb', 'a.b', 's'), \
                    regexp_matches('a\nb', 'a.b', ''), \
                    regexp_matches('a\nb', 'a.b', 'sm'), \
                    regexp_matches('a\nb', 'a.b', 'ms'), \
                    regexp_matches('a.b', 'a.b', 'l'), \
                    regexp_matches('axb', 'a.b', 'l');"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
        ]]
    );
    // Error paths, each with the linked model's message.
    let err = error(&mut db, "SELECT regexp_extract('abc', 'a(b)c', 10, '');");
    assert!(err.contains("group index must be between 0 and 9"), "{err}");
    let err = error(&mut db, "SELECT regexp_extract('abc', 'a(b)c', -1, '');");
    assert!(err.contains("group index must be between 0 and 9"), "{err}");
    let err = error(&mut db, "SELECT regexp_matches('a', 'a', 'g');");
    assert!(err.contains("regexp_replace"), "{err}");
    let err = error(&mut db, "SELECT regexp_matches('a', 'a', 'q');");
    assert!(err.contains("unrecognized regex option q"), "{err}");
    let err = error(&mut db, "SELECT regexp_matches('a', '(', '');");
    assert!(err.contains("invalid regular expression"), "{err}");
}

// ---------- composition: WHERE / GROUP BY / ORDER BY / built-ins ----------

#[test]
fn string_functions_compose_with_predicates_aggregates_and_builtins() {
    let mut db = test_db();
    db.execute(
        "CREATE TABLE words (word VARCHAR); \
         INSERT INTO words VALUES ('delta'), ('alpha'), ('beta');",
    )
    .unwrap();
    // WHERE with an extension predicate and an extension in a projection.
    assert_eq!(
        rows(
            &mut db,
            "SELECT upper(left(word, CAST(2 AS BIGINT))) FROM words \
             WHERE starts_with(word, 'a') OR strpos(word, 'et') > 0 ORDER BY 1;"
        ),
        vec![vec![text("AL")], vec![text("BE")]]
    );
    // GROUP BY an extension result (the grouped ORDER BY stays an alias).
    assert_eq!(
        rows(
            &mut db,
            "SELECT left(word, CAST(1 AS BIGINT)) AS initial, COUNT(*) \
             FROM words GROUP BY initial ORDER BY initial DESC;"
        ),
        vec![
            vec![text("d"), Value::Bigint(1)],
            vec![text("b"), Value::Bigint(1)],
            vec![text("a"), Value::Bigint(1)],
        ]
    );
    // ORDER BY an extension expression in a plain select.
    assert_eq!(
        rows(
            &mut db,
            "SELECT reverse(word) FROM words ORDER BY reverse(word);"
        ),
        vec![vec![text("ahpla")], vec![text("ateb")], vec![text("atled")],]
    );
    // Composition with string building and built-ins.
    assert_eq!(
        rows(
            &mut db,
            "SELECT '[' || reverse(split_part('a,b,c', ',', CAST(2 AS BIGINT))) || ']', \
                    length(reverse('héllo')) FROM words LIMIT 1;"
        ),
        vec![vec![text("[b]"), Value::Integer(5)]]
    );
}

// ---------- prepared statements ----------

#[test]
fn prepared_statements_bind_extension_arguments() {
    let mut db = test_db();
    let stmt = db
        .prepare("SELECT split_part(?, ',', CAST(? AS BIGINT)), strpos(?, reverse(?));")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 4);
    let out = db
        .execute_prepared(
            &stmt,
            &[
                text("a,b,c"),
                Value::Bigint(3),
                text("xhéllo"),
                text("olléh"),
            ],
        )
        .unwrap();
    match out {
        QueryResult::Selected { rows, .. } => assert_eq!(
            rows,
            vec![vec![text("c"), Value::Bigint(2)]],
            "split_part index 3 and strpos('xhéllo', reverse('olléh'))"
        ),
        other => panic!("{other:?}"),
    }
    // A BIGINT parameter round-trips its exact type: the substitution is
    // `CAST(-2 AS BIGINT)` and stays BIGINT.
    let stmt = db.prepare("SELECT left(?, CAST(? AS BIGINT));").unwrap();
    let out = db
        .execute_prepared(&stmt, &[text("hello"), Value::Bigint(-2)])
        .unwrap();
    match out {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![text("hel")]])
        }
        other => panic!("{other:?}"),
    }
    // An INTEGER parameter where ABI v1 declares BIGINT widens (DuckDB's
    // implicit cast); a VARCHAR one is a bind error naming the fix.
    let stmt = db.prepare("SELECT left(?, ?);").unwrap();
    match db
        .execute_prepared(&stmt, &[text("hello"), Value::Integer(2)])
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![text("he")]]),
        other => panic!("{other:?}"),
    }
    let err = db
        .execute_prepared(&stmt, &[text("hello"), text("2")])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("left argument 2 must be BIGINT, got VARCHAR (use CAST)"),
        "{err}"
    );
}

// ---------- strict typing for literals ----------

#[test]
fn re_registering_a_builtin_like_trim_is_refused_so_the_extension_omits_it() {
    // Evidence for the extension's biggest scope decision: `trim`/`ltrim`/
    // `rtrim`/`replace` are engine built-ins (DuckDB-pinned, differentially
    // tested) and an extension function cannot shadow one. The `string`
    // declaration therefore registers none of them — this pins the actual
    // refusal against the real registry.
    use pinkdb_ext_sdk::{ExtArg, ExtType, ExtValue, LinkedExtension, LinkedFunction};
    let clash = LinkedExtension::new(
        "wave53_trim_clash",
        vec![
            LinkedFunction::new(
                "trim",
                vec![ExtType::Varchar, ExtType::Varchar],
                ExtType::Varchar,
                |args: &[ExtArg<'_>]| match args.first() {
                    Some(ExtArg::Varchar(bytes)) => Ok(ExtValue::Varchar(bytes.to_vec())),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();
    pinkdb_ext_sdk::register_linked_extension(clash).expect("the registry accepts the extension");
    let mut db = PinkDb::new();
    let err = db
        .load_linked_extension("wave53_trim_clash")
        .expect_err("trim is a built-in")
        .to_string();
    assert!(
        err.contains("function \"trim\" conflicts with a built-in function"),
        "{err}"
    );
    // The built-in itself keeps working next to the extension.
    assert_eq!(
        rows(&mut db, "SELECT trim('  ab  '), trim('xxabxx', 'x');"),
        vec![vec![text("ab"), text("ab")]]
    );
}

#[test]
fn integer_literals_widen_to_bigint_and_strings_stay_strict() {
    let mut db = test_db();
    assert_eq!(
        rows(
            &mut db,
            "SELECT left('hello', 2), split_part('a,b', ',', 2);"
        ),
        vec![vec![text("he"), text("b")]]
    );
    let err = error(&mut db, "SELECT left('hello', '2');");
    assert!(
        err.contains("left argument 2 must be BIGINT, got VARCHAR (use CAST)"),
        "{err}"
    );
    // Argument count is exact.
    let err = error(&mut db, "SELECT reverse('a', 'b');");
    assert!(err.contains("takes 1 argument(s)"), "{err}");
}

// ---------- durable: deterministic functions replay from the WAL ----------

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_ext_string_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self { dir }
    }

    fn path(&self) -> String {
        self.dir.join("db.pink").to_str().unwrap().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn deterministic_functions_wal_log_and_replay_identically() {
    // Registration must precede the open: replay resolves the logged
    // `LOAD EXTENSION 'string'` against the process-global registry.
    register_once();
    let scratch = Scratch::new("replay");
    let (mut db, _info) = PinkDb::open_durable_with(&scratch.path(), SyncPolicy::Sync).unwrap();
    db.load_linked_extension("string").unwrap();
    db.execute("CREATE TABLE t (v VARCHAR);").unwrap();
    db.execute("CREATE TABLE src (w VARCHAR);").unwrap();
    db.execute("INSERT INTO src VALUES ('a,bc'), ('x,yz');")
        .unwrap();
    // A WAL-logged statement calling the extension: allowed because every
    // declared function is deterministic (a volatile one would be refused
    // before it runs).
    db.execute("INSERT INTO t SELECT reverse(split_part(w, ',', CAST(-1 AS BIGINT))) FROM src;")
        .unwrap();
    let before = rows(&mut db, "SELECT v FROM t ORDER BY v;");
    assert_eq!(before, vec![vec![text("cb")], vec![text("zy")]]);
    drop(db);
    // Reopen: recovery replays the WAL (no checkpoint), re-running the
    // INSERT through the extension callbacks.
    let (mut db, _info) = PinkDb::open_durable_with(&scratch.path(), SyncPolicy::Sync).unwrap();
    let after = rows(&mut db, "SELECT v FROM t ORDER BY v;");
    assert_eq!(after, before, "WAL replay diverged");
}

// ---------- DuckDB's optional arities, padding and concat_ws ----------

#[test]
fn regexp_arities_and_replace_match_duckdb() {
    let mut db = test_db();
    // DuckDB's optional trailing arguments: matches 2/3, extract 2/3/4.
    assert_eq!(
        rows(
            &mut db,
            "SELECT regexp_matches('abc', 'b'), regexp_matches('ABC', 'b'), \
             regexp_matches('ABC', 'b', 'i'), regexp_extract('abc', 'b'), \
             regexp_extract('abc', 'b', 1), \
             regexp_extract('abc-123', '([a-z]+)-([0-9]+)', 2), \
             regexp_extract('abc', '(b)', 1, 'i');"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
            text("b"),
            text(""),
            text("123"),
            text("b"),
        ]]
    );
    // regexp_replace rewrites the first match unless the `g` option is given,
    // and `\1`/`\0` are group references.
    assert_eq!(
        rows(
            &mut db,
            "SELECT regexp_replace('abcabc', 'b', 'x'), \
             regexp_replace('abcabc', 'b', 'x', 'g'), \
             regexp_replace('abcabc', '(b)', '[\\1]'), \
             regexp_replace('abcabc', '(b)', '[\\1]', 'g'), \
             regexp_replace('abc', 'b', '<\\0>'), \
             regexp_replace('aBcB', 'b', 'x', 'gi'), \
             regexp_replace('abc', '', '-', 'g');"
        ),
        vec![vec![
            text("axcabc"),
            text("axcaxc"),
            text("a[b]cabc"),
            text("a[b]ca[b]c"),
            text("a<b>c"),
            text("axcx"),
            text("-a-b-c-"),
        ]]
    );
    // NULL input/pattern/replacement are NULL; a NULL options field is not.
    for sql in [
        "SELECT regexp_replace(NULL, 'b', 'x') IS NULL;",
        "SELECT regexp_replace('abc', NULL, 'x') IS NULL;",
        "SELECT regexp_replace('abc', 'b', NULL) IS NULL;",
    ] {
        assert_eq!(
            rows(&mut db, sql),
            vec![vec![Value::Boolean(true)]],
            "{sql}"
        );
    }
    let err = error(&mut db, "SELECT regexp_replace('abc', 'b', 'x', NULL);");
    assert!(err.contains("regex options must not be NULL"), "{err}");
    // A bad rewrite schema fails even when the pattern never matches, like
    // DuckDB validating the schema up front.
    let err = error(&mut db, "SELECT regexp_replace('xyz', 'b', '\\q');");
    assert!(err.contains("must be followed by a digit"), "{err}");
    let err = error(&mut db, "SELECT regexp_replace('abc', 'b', 'x', 'G');");
    assert!(err.contains("unrecognized regex option G"), "{err}");
    // DuckDB validates the options before it looks at NULL inputs.
    for (sql, needle) in [
        (
            "SELECT regexp_matches('a', NULL, 'q');",
            "unrecognized regex option q",
        ),
        (
            "SELECT regexp_replace(NULL, 'b', 'c', 'q');",
            "unrecognized regex option q",
        ),
        ("SELECT regexp_matches(NULL, 'a', 'g');", "regexp_replace"),
    ] {
        let err = error(&mut db, sql);
        assert!(err.contains(needle), "{sql}: {err}");
    }
    // Arity errors name the DuckDB range.
    for (sql, needle) in [
        (
            "SELECT regexp_matches('a');",
            "takes between 2 and 3 argument(s)",
        ),
        (
            "SELECT regexp_extract('a');",
            "takes between 2 and 4 argument(s)",
        ),
        (
            "SELECT regexp_replace('a', 'b');",
            "takes between 3 and 4 argument(s)",
        ),
        (
            "SELECT regexp_replace('a', 'b', 'c', 'g', 'x');",
            "takes at most 4 argument(s)",
        ),
    ] {
        let err = error(&mut db, sql);
        assert!(err.contains(needle), "{sql}: {err}");
    }
}

#[test]
fn padding_and_concat_ws_match_duckdb() {
    let mut db = test_db();
    // Padding repeats the pad string (character-wise), and truncation wins
    // over an empty pad.
    assert_eq!(
        rows(
            &mut db,
            "SELECT lpad('a', 3, 'x'), lpad('a', 3, 'xyz'), lpad('abcdef', 3, 'x'), \
             lpad('a', 5, 'ab'), lpad('a', 5, 'abc'), lpad('héllo', 6, 'x'), \
             lpad('a', 0, 'x'), lpad('a', -3, 'x'), lpad('abc', 2, ''), \
             rpad('a', 3, 'x'), rpad('a', 5, 'abc'), rpad('', 2, 'ab'), \
             rpad('abcdef', 3, 'x');"
        ),
        vec![vec![
            text("xxa"),
            text("xya"),
            text("abc"),
            text("ababa"),
            text("abcaa"),
            text("xhéllo"),
            text(""),
            text(""),
            text("ab"),
            text("axx"),
            text("aabca"),
            text("ab"),
            text("abc"),
        ]]
    );
    let err = error(&mut db, "SELECT lpad('a', 3, '');");
    assert!(err.contains("Insufficient padding in LPAD"), "{err}");
    let err = error(&mut db, "SELECT rpad('a', 3, '');");
    assert!(err.contains("Insufficient padding in RPAD"), "{err}");
    for sql in [
        "SELECT lpad(NULL, 3, 'x') IS NULL;",
        "SELECT lpad('a', NULL, 'x') IS NULL;",
        "SELECT lpad('a', 3, NULL) IS NULL;",
        "SELECT rpad('a', NULL, 'x') IS NULL;",
    ] {
        assert_eq!(
            rows(&mut db, sql),
            vec![vec![Value::Boolean(true)]],
            "{sql}"
        );
    }
    // concat_ws skips NULL arguments; a NULL separator poisons the row.
    assert_eq!(
        rows(
            &mut db,
            "SELECT concat_ws(',', 'a', NULL, 'b'), concat_ws(',', NULL, NULL), \
             concat_ws('', 'a', 'b'), concat_ws(',', 'a'), concat_ws(',', 'a', 'b', 'c');"
        ),
        vec![vec![
            text("a,b"),
            text(""),
            text("ab"),
            text("a"),
            text("a,b,c"),
        ]]
    );
    assert_eq!(
        rows(&mut db, "SELECT concat_ws(NULL, 'a') IS NULL;"),
        vec![vec![Value::Boolean(true)]]
    );
    // DuckDB's arity rules: lpad needs 3 arguments, concat_ws at least 2.
    let err = error(&mut db, "SELECT lpad('a', CAST(2 AS BIGINT));");
    assert!(err.contains("takes 3 argument(s)"), "{err}");
    let err = error(&mut db, "SELECT concat_ws(',');");
    assert!(err.contains("at least 2 argument(s)"), "{err}");
}
