//! Error bodies and the prefixes DuckDB's own errors carry.

use pinkdb::types::PinkDbError;

use crate::encode::push_json_string;

/// `{"error":{"code":…,"message":…}}`, the plan's only error shape.
pub(crate) fn error_body(code: &str, message: &str) -> String {
    let mut out = String::from("{\"error\":{\"code\":");
    push_json_string(&mut out, code);
    out.push_str(",\"message\":");
    push_json_string(&mut out, message);
    out.push_str("}}");
    out
}

/// The engine's message under the prefix DuckDB would give the same kind of
/// error, which is what QuackLab keys on.
///
/// The wording after the prefix stays pinkdb's: QuackLab matches substrings
/// "not found" / "already exists" / "duplicate key", and pinkdb says those.
/// An error that already carries a prefix is never prefixed twice.
pub(crate) fn engine_message(err: &PinkDbError) -> String {
    let message = err.to_string();
    let prefix = match err {
        PinkDbError::Parse(_) => "Parser Error: ",
        PinkDbError::Catalog(_) => "Catalog Error: ",
        PinkDbError::Type(_) => "Binder Error: ",
        _ => {
            // Execution, Parameter, Permission, Io and Csv all come out of the
            // executor; DuckDB's own wording is the discriminator there.
            let lower = message.to_ascii_lowercase();
            if lower.contains("constraint") {
                "Constraint Error: "
            } else if lower.contains("transaction") || lower.contains("write conflict") {
                "TransactionContext Error: "
            } else if lower.contains("cast") || lower.contains("convert") {
                "Conversion Error: "
            } else {
                "Invalid Input Error: "
            }
        }
    };
    if message.starts_with(prefix.trim_end()) {
        message
    } else {
        format!("{prefix}{message}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_get_duckdbs_prefixes() {
        let parse = PinkDbError::Parse("syntax error at or near \"SELEC\"".to_string());
        assert!(engine_message(&parse).starts_with("Parser Error: "));
        let catalog = PinkDbError::Catalog("table \"nope\" not found".to_string());
        assert!(engine_message(&catalog).starts_with("Catalog Error: "));
        let cast = PinkDbError::Execution("cannot CAST \"a\" AS INTEGER".to_string());
        assert_eq!(
            engine_message(&cast),
            "Conversion Error: cannot CAST \"a\" AS INTEGER"
        );
        let constraint = PinkDbError::Execution(
            "PRIMARY KEY or UNIQUE constraint violation: duplicate key \"a\"".to_string(),
        );
        assert!(engine_message(&constraint).starts_with("Constraint Error: "));
        let txn = PinkDbError::Execution("no transaction in progress".to_string());
        assert!(engine_message(&txn).starts_with("TransactionContext Error: "));
    }

    #[test]
    fn a_prefix_is_never_doubled() {
        let already = PinkDbError::Parse("Parser Error: bad".to_string());
        assert_eq!(engine_message(&already), "Parser Error: bad");
    }

    #[test]
    fn bodies_escape_like_go() {
        assert_eq!(
            error_body("unauthorized", "a <b> & c"),
            "{\"error\":{\"code\":\"unauthorized\",\"message\":\"a \\u003cb\\u003e \\u0026 c\"}}"
        );
    }
}
