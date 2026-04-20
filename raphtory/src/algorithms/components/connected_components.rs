use crate::{
    db::{
        api::{
            state::{GenericNodeState, Index, TypedNodeState},
            view::{internal::GraphView, NodeViewOps, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use disjoint_sets::AUnionFind;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug, Default, Hash, Eq)]
pub struct ConnectedComponent {
    pub component_id: usize,
}

impl PartialEq<usize> for ConnectedComponent {
    fn eq(&self, other: &usize) -> bool {
        self.component_id == *other
    }
}

/// Computes the connected community_detection of a graph using the Simple Connected Components algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `iter_count` - The number of iterations to run
/// * `threads` - Number of threads to use
///
/// # Returns
///
/// An [NodeState] containing the mapping from each node to its component ID
///
pub fn weakly_connected_components<G>(g: &G) -> TypedNodeState<'static, ConnectedComponent, G>
where
    G: StaticGraphViewOps,
{
    let index = Index::for_graph(g.clone());
    let dss = AUnionFind::new(index.len());
    g.nodes().par_iter().for_each(|node| {
        let src_node: usize = index.index(&node.node).unwrap();
        node.out_neighbours().iter().for_each(|nbor| {
            dss.union(src_node, index.index(&nbor.node).unwrap());
        })
    });
    let result = TypedNodeState::new(GenericNodeState::new_from_eval_with_index_mapped(
        g.clone(),
        dss.to_vec(),
        index,
        |value| ConnectedComponent {
            component_id: value,
        },
        None,
    ));
    result
}
