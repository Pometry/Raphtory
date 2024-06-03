use crate::{
    core::entities::VID,
    disk_graph::storage_interface::{node::DiskNode, nodes_ref::DiskNodesRef},
};

use pometry_storage::graph::TemporalGraph;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct DiskNodesOwned {
    graph: Arc<TemporalGraph>,
}

impl DiskNodesOwned {
    pub(crate) fn new(graph: Arc<TemporalGraph>) -> Self {
        Self { graph }
    }

    pub fn node(&self, vid: VID) -> DiskNode {
        DiskNode::new(&self.graph, vid)
    }

    pub fn as_ref(&self) -> DiskNodesRef {
        DiskNodesRef::new(&self.graph)
    }
}
