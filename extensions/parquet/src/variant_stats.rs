//! Path-qualified row-group statistics of a shredded VARIANT column.
//!
//! A shredded column's own min/max would have to compare whole objects, so
//! the useful bounds are the ones per path: the statistics of the typed leaf
//! under `typed_value` bound every `v['a']` of the row group.
//!
//! Only files pinkdb wrote are mined. A path's typed leaf is a *complete*
//! bound only when every non-NULL value of that path had the leaf's type; a
//! foreign writer may leave a value of another type in the residual `value`
//! buffer, which the leaf reports as NULL — a min/max that silently excludes
//! such a value. pinkdb's writer infers each column's policy over the whole
//! column, so its bounds are exact, and it says so with the
//! [`COMPLETE_KEY`] footer key. Foreign files get no footer-derived path
//! statistics (the lake's own statistics block still prunes them).

use std::sync::Arc;

use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use pinkdb::Value;
use pinkdb::variant::{Step, Variant, stats_path_text};
use pinkdb::variant_shred::ShredKind;

use crate::file_meta::ColStats;
use crate::variant_read::{Node, Shredded, VariantColumn};

/// The footer key a pinkdb writer sets when every VARIANT column of the file
/// was shredded with a policy inferred over the whole column, so each typed
/// leaf bounds every value of its path.
pub const COMPLETE_KEY: &str = "pinkdb.variant.complete";

/// The value pinkdb writes for [`COMPLETE_KEY`]: the shredding format version.
pub const COMPLETE_VALUE: &str = "1";

/// Every path of `column` this row group can bound, with its statistics
/// (paths in shredding order).
pub fn column_paths(
    column: &VariantColumn,
    rg: &RowGroupMetaData,
    first_leaf: usize,
    complete: bool,
) -> Vec<(String, ColStats)> {
    let mut out = Vec::new();
    if !complete {
        return out;
    }
    for (name, field) in &column.keys {
        walk(
            field,
            vec![Step::Key(Arc::clone(name))],
            rg,
            first_leaf,
            &mut out,
        );
    }
    out
}

/// Recurse into a shredded node: a primitive leaf contributes its chunk's
/// statistics, a shredded object one entry per field's path, an array
/// nothing (its element bounds are per repetition, not per row).
fn walk(
    shredded: &Shredded,
    steps: Vec<Step>,
    rg: &RowGroupMetaData,
    first_leaf: usize,
    out: &mut Vec<(String, ColStats)>,
) {
    match &shredded.node {
        Node::Primitive { leaf, kind, .. } => {
            let Some(stats) = rg.column(first_leaf + *leaf).statistics() else {
                return;
            };
            let Some(path) = stats_path_text(&steps) else {
                return;
            };
            out.push((path, leaf_stats(stats, *kind)));
        }
        Node::Object(fields) => {
            for (name, field) in fields {
                if matches!(field.node, Node::Absent) {
                    continue;
                }
                let mut child = steps.clone();
                child.push(Step::Key(Arc::clone(name)));
                walk(field, child, rg, first_leaf, out);
            }
        }
        Node::Array(_) | Node::Absent => {}
    }
}

/// One leaf's statistics as path bounds. A path with no non-NULL value has no
/// bounds, which is what the pruner reads as "nothing to exclude".
fn leaf_stats(stats: &Statistics, kind: ShredKind) -> ColStats {
    let (min, max) = bounds(stats, kind);
    ColStats {
        min,
        max,
        null_count: stats.null_count_opt(),
        min_exact: stats.min_is_exact(),
        max_exact: stats.max_is_exact(),
    }
}

/// The leaf's typed min/max as VARIANT values.
fn bounds(stats: &Statistics, kind: ShredKind) -> (Option<Value>, Option<Value>) {
    let both = |pair: Option<(Variant, Variant)>| match pair {
        Some((lo, hi)) => (
            Some(Value::Variant(Arc::new(lo))),
            Some(Value::Variant(Arc::new(hi))),
        ),
        None => (None, None),
    };
    match (stats, kind) {
        (Statistics::Boolean(s), ShredKind::Bool) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Bool(*lo), Variant::Bool(*hi))),
        ),
        (Statistics::Int32(s), ShredKind::Int64) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Int(*lo as i64), Variant::Int(*hi as i64))),
        ),
        (Statistics::Int32(s), ShredKind::Date) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Date(*lo), Variant::Date(*hi))),
        ),
        (Statistics::Int64(s), ShredKind::Int64) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Int(*lo), Variant::Int(*hi))),
        ),
        (Statistics::Int64(s), ShredKind::Timestamp) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Timestamp(*lo), Variant::Timestamp(*hi))),
        ),
        (Statistics::Double(s), ShredKind::Double) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Double(*lo), Variant::Double(*hi))),
        ),
        (Statistics::Float(s), ShredKind::Double) => both(
            s.min_opt()
                .zip(s.max_opt())
                .map(|(lo, hi)| (Variant::Double(*lo as f64), Variant::Double(*hi as f64))),
        ),
        (Statistics::ByteArray(s), ShredKind::Str) => {
            let text = |value: &parquet::data_type::ByteArray| {
                String::from_utf8(value.data().to_vec())
                    .ok()
                    .map(Arc::<str>::from)
            };
            both(
                s.min_opt()
                    .zip(s.max_opt())
                    .and_then(|(lo, hi)| Some((Variant::Str(text(lo)?), Variant::Str(text(hi)?)))),
            )
        }
        // A kind/physical mismatch means the leaf is not the one the policy
        // described: publish nothing rather than a wrong bound.
        _ => (None, None),
    }
}
