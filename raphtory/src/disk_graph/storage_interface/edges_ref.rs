use crate::{
    core::entities::{LayerIds, EID},
    db::api::storage::graph::{
        edges::edge_storage_ops::EdgeStorageOps, variants::layer_variants::LayerVariants,
    },
    disk_graph::storage_interface::edge::DiskEdge,
};
use pometry_storage::graph::TemporalGraph;
use rayon::prelude::*;
use std::iter;

#[derive(Copy, Clone, Debug)]
pub struct DiskEdgesRef<'a> {
    pub(super) graph: &'a TemporalGraph,
}

impl<'a> DiskEdgesRef<'a> {
    pub(crate) fn new(storage: &'a TemporalGraph) -> Self {
        Self { graph: storage }
    }

    pub fn edge(self, eid: EID) -> DiskEdge<'a> {
        self.graph.edge(eid)
    }

    pub fn iter(self, layers: &LayerIds) -> impl Iterator<Item = DiskEdge<'a>> + use<'a, '_> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(self.graph.edges_iter()),
            LayerIds::One(layer_id) => LayerVariants::One(self.graph.edges_layer_iter(*layer_id)),
            layer_ids => LayerVariants::Multiple(
                self.graph
                    .edges_iter()
                    .filter(move |e| e.has_layer(layer_ids)),
            ),
        }
    }

    pub fn par_iter(
        self,
        layers: &LayerIds,
    ) -> impl ParallelIterator<Item = DiskEdge<'a>> + use<'a, '_> {
        match layers {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(self.graph.edges_par_iter()),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.graph.edges_layer_par_iter(*layer_id))
            }
            layer_ids => LayerVariants::Multiple(
                self.graph
                    .edges_par_iter()
                    .filter(move |e| e.has_layer(layer_ids)),
            ),
        }
    }

    pub fn count(self, layers: &LayerIds) -> usize {
        match layers {
            LayerIds::None => 0,
            LayerIds::All => self.graph.num_edges(),
            LayerIds::One(id) => self.graph.layer(*id).num_edges(),
            layer_ids => self
                .graph
                .edges_par_iter()
                .filter(move |e| e.has_layer(layer_ids))
                .count(),
        }
    }

    pub fn len(&self) -> usize {
        self.count(&LayerIds::All)
    }
}
