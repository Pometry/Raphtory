use crate::{
    db::api::{
        state::{GenericNodeState, Index, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::GraphViewOps,
};
use indexmap::IndexSet;
use raphtory_api::core::entities::VID;
use serde::{Deserialize, Serialize};

/// This is a mock algorithm for test purposes only!
/// A per-node boolean mask value.
///
/// Computes a deterministic boolean mask over nodes, alternating `true/false`
/// by **node iteration order**.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlternatingMask {
    pub bool_col: bool,
}

pub fn alternating_mask<G: StaticGraphViewOps>(
    g: &G,
) -> TypedNodeState<'static, AlternatingMask, G> {
    let mut index = IndexSet::<VID, ahash::RandomState>::default();
    let mut values = Vec::new();

    for (i, node) in g.nodes().iter().enumerate() {
        index.insert(node.node);
        values.push(AlternatingMask {
            bool_col: i % 2 != 0,
        });
    }

    let state = GenericNodeState::new_from_eval_with_index(g.clone(), values, Index::new(index), None);
    TypedNodeState::new(state)
}
