use crate::{core::entities::VID, disk_graph::storage_interface::node::DiskNode};
use pometry_storage::graph::TemporalGraph;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

#[derive(Copy, Clone, Debug)]
pub struct DiskNodesRef<'a> {
    graph: &'a TemporalGraph,
}

impl<'a> DiskNodesRef<'a> {
    pub(crate) fn new(graph: &'a TemporalGraph) -> Self {
        Self { graph }
    }

    pub fn node(self, vid: VID) -> DiskNode<'a> {
        DiskNode::new(self.graph, vid)
    }

    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = DiskNode<'a>> {
        (0..self.graph.num_nodes())
            .into_par_iter()
            .map(move |vid| self.node(VID(vid)))
    }

    pub fn iter(self) -> impl Iterator<Item = DiskNode<'a>> {
        (0..self.graph.num_nodes()).map(move |vid| self.node(VID(vid)))
    }
}
