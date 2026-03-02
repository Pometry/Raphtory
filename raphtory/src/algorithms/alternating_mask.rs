use crate::{
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    let mut map: HashMap<VID, AlternatingMask> = HashMap::new();

    for (i, node) in g.nodes().iter().enumerate() {
        map.insert(
            node.node,
            AlternatingMask {
                bool_col: i % 2 != 0,
            },
        );
    }

    let state = GenericNodeState::new_from_map(g.clone(), map, |v| v, None);
    TypedNodeState::new(state)
}
