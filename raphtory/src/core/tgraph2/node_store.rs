use crate::core::timeindex::TimeIndex;

use super::edge_layer::EdgeLayer;


pub(crate) struct NodeStore<const N: usize> {
    global_id: u64,
    // all the timestamps that have been seen by this vertex
    timestamps: TimeIndex,
    // each layer represents a separate view of the graph
    layers: Vec<EdgeLayer>,
}