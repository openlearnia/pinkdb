//! Hive-style partition columns, pinned against `duckdb` v2.0.0-alpha46057.
//!
//! Every path component of the form `key=value` under the scan's matched
//! files contributes a hive column: the pair nearest the file is the deepest
//! partition. The scan's hive columns are the **union** of the keys seen
//! across all files, in **alphabetical order**, appended after the file's own
//! columns (and after `file_row_number`/`filename`) — α's order.
//!
//! Values are VARCHAR unless `hive_partitioning` is on and the value
//! round-trips through the inferred type (α's rule, `hive_types_autocast`):
//!
//! | value | type |
//! |---|---|
//! | `us` | VARCHAR |
//! | `2024` | BIGINT |
//! | `2024-01-01` | DATE |
//! | `01`, `0012`, `true`, `1.5`, `2.0` | VARCHAR (no round-trip) |
//!
//! The round-trip rule is what makes `01` VARCHAR: it parses as BIGINT, but
//! `1` does not format back to `01`. There is deliberately no DOUBLE
//! inference (`1.5` stays VARCHAR) and no leading-zero integer inference.

use pinkdb::{DataType, Value};

use crate::parse::{parse_date, parse_i64};

/// The hive pairs a file's path contributes: `(key, value)` in path order.
pub fn partition_pairs(path: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for component in path.split('/') {
        if component.is_empty() {
            continue;
        }
        // A key is everything before the first `=`; the value may contain
        // further `=` characters (α splits on the first only).
        let Some((key, value)) = component.split_once('=') else {
            continue;
        };
        if key.is_empty() {
            continue;
        }
        out.push((key.to_string(), value.to_string()));
    }
    out
}

/// One hive column of a resolved scan: its key and the type α infers from the
/// values seen (BIGINT / DATE / VARCHAR).
#[derive(Debug, Clone, PartialEq)]
pub struct HiveColumn {
    pub key: String,
    pub ty: DataType,
}

/// The inferred type of one hive key across the values of every file that has
/// it (α infers per value, then unifies: any mismatch falls back to VARCHAR).
pub fn infer_type(values: impl Iterator<Item = String>) -> DataType {
    let mut ty: Option<DataType> = None;
    for value in values {
        let this = infer_value(&value);
        ty = Some(match ty {
            None => this,
            Some(prev) if prev == this => prev,
            Some(prev) => match (prev, this) {
                // BIGINT/DATE never mix without a fallback; keep it simple and
                // α-shaped: any disagreement is VARCHAR.
                (DataType::Varchar, _) | (_, DataType::Varchar) => DataType::Varchar,
                _ => DataType::Varchar,
            },
        });
    }
    ty.unwrap_or(DataType::Varchar)
}

/// α's per-value inference: BIGINT if the text is the canonical decimal form
/// of an i64, DATE if the text is `YYYY-MM-DD`, else VARCHAR.
fn infer_value(value: &str) -> DataType {
    if let Some(n) = parse_i64(value)
        && n.to_string() == value
    {
        return DataType::Bigint;
    }
    if parse_date(value).is_some() {
        return DataType::Date;
    }
    DataType::Varchar
}

/// The engine value of one hive cell under `ty`, or VARCHAR text when the
/// value does not convert (a file whose key inherited a type from a sibling).
pub fn value_of(ty: DataType, text: &str) -> Value {
    match ty {
        DataType::Bigint => parse_i64(text)
            .map(Value::Bigint)
            .unwrap_or_else(|| Value::Varchar(text.to_string())),
        DataType::Date => parse_date(text)
            .map(Value::Date)
            .unwrap_or_else(|| Value::Varchar(text.to_string())),
        _ => Value::Varchar(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_are_read_from_path_components() {
        assert_eq!(
            partition_pairs("fx/part/region=us/year=2024/u.parquet"),
            vec![
                ("region".to_string(), "us".to_string()),
                ("year".to_string(), "2024".to_string())
            ]
        );
        assert_eq!(partition_pairs("u.parquet"), Vec::<(String, String)>::new());
        // Only the first `=` splits.
        assert_eq!(
            partition_pairs("a=x=y/f.parquet"),
            vec![("a".to_string(), "x=y".to_string())]
        );
    }

    #[test]
    fn inference_matches_the_probed_rules() {
        assert_eq!(
            infer_type(["2024".to_string()].into_iter()),
            DataType::Bigint
        );
        assert_eq!(infer_type(["-3".to_string()].into_iter()), DataType::Bigint);
        assert_eq!(
            infer_type(["us".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["01".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["0012".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["1.5".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["2.0".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["true".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(
            infer_type(["2024-01-01".to_string()].into_iter()),
            DataType::Date
        );
        // Mixed values fall back to VARCHAR.
        assert_eq!(
            infer_type(["2024".to_string(), "us".to_string()].into_iter()),
            DataType::Varchar
        );
        assert_eq!(infer_type(std::iter::empty()), DataType::Varchar);
    }

    #[test]
    fn values_convert_under_the_inferred_type() {
        assert_eq!(value_of(DataType::Bigint, "2024"), Value::Bigint(2024));
        assert_eq!(value_of(DataType::Date, "2024-01-01"), Value::Date(19723));
        assert_eq!(
            value_of(DataType::Varchar, "us"),
            Value::Varchar("us".to_string())
        );
        // A value that does not fit the type stays text (never a wrong cast).
        assert_eq!(
            value_of(DataType::Bigint, "us"),
            Value::Varchar("us".to_string())
        );
    }
}
