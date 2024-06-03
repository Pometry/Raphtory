use crate::{
    disk_graph::storage_interface::{node::ArrowNode, nodes_ref::ArrowNodesRef},
    core::entities::VID,
};

use pometry_storage::graph::TemporalGraph;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ArrowNodesOwned {
    graph: Arc<TemporalGraph>,
}

impl ArrowNodesOwned {
    pub(crate) fn new(graph: Arc<TemporalGraph>) -> Self {
        Self { graph }
    }

    pub fn node(&self, vid: VID) -> ArrowNode {
        ArrowNode::new(&self.graph, vid)
    }

    pub fn as_ref(&self) -> ArrowNodesRef {
        ArrowNodesRef::new(&self.graph)
    }
}
