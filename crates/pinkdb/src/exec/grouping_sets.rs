//! GROUPING SETS / ROLLUP / CUBE execution.
//!
//! One input scan feeds every set: key and aggregate arguments are evaluated
//! once per chunk, then each set folds the same rows into its own hash table
//! (DuckDB's `PhysicalHashAggregate` grouping-sets layout). A set emits its
//! real keys with NULLs where the set omits one, followed by the constant
//! `GROUPING()` masks, so the result is one ordinary aggregate output.

use super::Cx;
use super::aggregate::{
    EvalVec, Group, Partial, accumulate, emit_groups, eval_agg_vec, group_ids, keep_mask,
};
use crate::plan::GroupingSetPlan;
use crate::sql::binder::{BoundAgg, BoundExpr};
use crate::types::{DataType, Result, Value};
use crate::vector::{DataChunk, Vector};

/// Run every expanded grouping set over `inputs` in one pass.
pub(crate) fn aggregate(
    cx: &Cx,
    inputs: &[DataChunk],
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
    plan: &GroupingSetPlan,
) -> Result<Vec<DataChunk>> {
    let mut partials: Vec<Partial> = plan
        .sets
        .iter()
        .map(|_| Partial::for_rows(0, cx.budget.memory()))
        .collect();
    for chunk in inputs {
        if chunk.num_rows() == 0 {
            continue;
        }
        cx.check_deadline()?;
        let key_vecs: Vec<EvalVec> = keys[..plan.key_count]
            .iter()
            .map(|k| eval_agg_vec(k, chunk, cx))
            .collect::<Result<_>>()?;
        let key_refs: Vec<&Vector> = key_vecs.iter().map(|v| v.v()).collect();
        let arg_vecs: Vec<Option<EvalVec>> = aggs
            .iter()
            .map(|a| match &a.arg {
                Some(e) => eval_agg_vec(e, chunk, cx).map(Some),
                None => Ok(None),
            })
            .collect::<Result<_>>()?;
        let keeps: Vec<Option<Vec<bool>>> = aggs
            .iter()
            .map(|a| match &a.filter {
                Some(pred) => keep_mask(eval_agg_vec(pred, chunk, cx)?.v()).map(Some),
                None => Ok(None),
            })
            .collect::<Result<_>>()?;
        for (set, partial) in plan.sets.iter().zip(partials.iter_mut()) {
            let set_keys: Vec<&Vector> = set.iter().map(|&i| key_refs[i]).collect();
            let gids = group_ids(partial, &set_keys, aggs, chunk.num_rows(), None);
            for (j, av) in arg_vecs.iter().enumerate() {
                accumulate(
                    partial.groups.as_mut_slice(),
                    &gids,
                    j,
                    av.as_ref().map(|v| v.v()),
                    keeps[j].as_deref(),
                    None,
                );
            }
            partial.finish_chunk()?;
        }
    }

    // A keyless set emits its grand-total row even when the input was empty.
    for (set, partial) in plan.sets.iter().zip(partials.iter_mut()) {
        if set.is_empty() {
            partial.ensure_empty_group(aggs);
        }
    }

    let key_types: Vec<DataType> = keys.iter().map(|k| k.dtype()).collect();
    let mut groups = Vec::new();
    for (i, partial) in partials.into_iter().enumerate() {
        let set = &plan.sets[i];
        let masks = &plan.masks[i];
        for (_, group) in partial.groups {
            let mut out = Vec::with_capacity(key_types.len());
            let mut at = 0usize;
            for key in 0..plan.key_count {
                if set.binary_search(&key).is_ok() {
                    out.push(group.keys[at].clone());
                    at += 1;
                } else {
                    out.push(Value::Null);
                }
            }
            out.extend(masks.iter().map(|m| Value::Bigint(*m)));
            groups.push(Group {
                keys: out,
                accs: group.accs,
            });
        }
    }
    emit_groups(groups, &key_types, aggs)
}
