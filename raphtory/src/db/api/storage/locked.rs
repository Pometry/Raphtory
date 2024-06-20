use std::sync::Arc;

use crate::core::{
    entities::{graph::edges::LockedEdges, nodes::node_store::NodeStore, VID},
    storage::ReadLockedStorage,
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
