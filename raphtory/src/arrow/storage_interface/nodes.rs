use crate::{
    arrow::storage_interface::{node::ArrowNode, nodes_ref::ArrowNodesRef},
    core::entities::VID,
};

use raphtory_arrow::{
    graph::TemporalGraph, graph_fragment::TempColGraphFragment, properties::Properties,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ArrowNodesOwned {
    num_nodes: usize,
    properties: Option<Properties<VID>>,
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowNodesOwned {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            num_nodes: graph.num_nodes(),
            properties: graph.node_properties().cloned(),
            layers: graph.layers().into(),
        }
    }

    pub fn node(&self, vid: VID) -> ArrowNode {
        ArrowNode {
            properties: self.properties.as_ref(),
            layers: &self.layers,
            vid,
        }
    }

    pub fn as_ref(&self) -> ArrowNodesRef {
        ArrowNodesRef {
            num_nodes: self.num_nodes,
            properties: self.properties.as_ref(),
            layers: &self.layers,
        }
    }
}
