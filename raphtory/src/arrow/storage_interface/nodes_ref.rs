use crate::{arrow::storage_interface::node::ArrowNode, core::entities::VID};
use pometry_storage::graph::TemporalGraph;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

#[derive(Copy, Clone, Debug)]
pub struct ArrowNodesRef<'a> {
    graph: &'a TemporalGraph,
}

impl<'a> ArrowNodesRef<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph) -> Self {
        Self { graph }
    }

    pub fn node(self, vid: VID) -> ArrowNode<'a> {
        ArrowNode::new(self.graph, vid)
    }

    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = ArrowNode<'a>> {
        (0..self.graph.num_nodes())
            .into_par_iter()
            .map(move |vid| self.node(VID(vid)))
    }

    pub fn iter(self) -> impl Iterator<Item = ArrowNode<'a>> {
        (0..self.graph.num_nodes()).map(move |vid| self.node(VID(vid)))
    }
}
