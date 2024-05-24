use crate::{arrow::storage_interface::node::ArrowNode, core::entities::VID};
use raphtory_arrow::{
    graph::TemporalGraph, graph_fragment::TempColGraphFragment, properties::Properties,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
pub struct ArrowNodesRef<'a> {
    pub(super) num_nodes: usize,
    pub(super) properties: Option<&'a Properties<VID>>,
    pub(super) layers: &'a Arc<[TempColGraphFragment]>,
}

impl<'a> ArrowNodesRef<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph) -> Self {
        Self {
            num_nodes: graph.num_nodes(),
            properties: graph.node_properties(),
            layers: &graph.arc_layers(),
        }
    }

    pub fn node(self, vid: VID) -> ArrowNode<'a> {
        ArrowNode {
            properties: self.properties,
            layers: self.layers,
            vid,
        }
    }

    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = ArrowNode<'a>> {
        (0..self.num_nodes)
            .into_par_iter()
            .map(move |vid| self.node(VID(vid)))
    }

    pub fn iter(self) -> impl Iterator<Item = ArrowNode<'a>> {
        (0..self.num_nodes).map(move |vid| self.node(VID(vid)))
    }
}
