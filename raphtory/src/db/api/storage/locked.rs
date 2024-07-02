use std::sync::Arc;

use crate::core::{
    entities::{nodes::node_store::NodeStore, VID},
    storage::{raw_edges::LockedEdges, ReadLockedStorage},
};

#[derive(Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedStorage<NodeStore, VID>>,
    pub(crate) edges: Arc<LockedEdges>,
}

impl Clone for LockedGraph {
    fn clone(&self) -> Self {
        LockedGraph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        }
    }
}
