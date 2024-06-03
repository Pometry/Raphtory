use crate::{
    disk_graph::storage_interface::edge::ArrowEdge,
    core::entities::{LayerIds, EID},
    db::api::storage::variants::layer_variants::LayerVariants,
};
use pometry_storage::{graph::TemporalGraph, graph_fragment::TempColGraphFragment};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::iter;

#[derive(Copy, Clone, Debug)]
pub struct ArrowEdgesRef<'a> {
    pub(super) layers: &'a [TempColGraphFragment],
}

impl<'a> ArrowEdgesRef<'a> {
    pub(crate) fn new(storage: &'a TemporalGraph) -> Self {
        Self {
            layers: storage.layers(),
        }
    }

    pub fn edge(self, eid: EID, layer_id: usize) -> ArrowEdge<'a> {
        self.layers[layer_id].edge(eid)
    }

    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = ArrowEdge<'a>> {
        match layers {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(
                self.layers
                    .iter()
                    .flat_map(|layer| layer.edges_storage().iter()),
            ),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.layers[layer_id].edges_storage().iter())
            }
            LayerIds::Multiple(ids) => {
                let ids = ids.clone();
                LayerVariants::Multiple(
                    (0..ids.len()).flat_map(move |i| self.layers[ids[i]].edges_storage().iter()),
                )
            }
        }
    }

    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = ArrowEdge<'a>> {
        match layers {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(
                self.layers
                    .par_iter()
                    .flat_map(|layer| layer.edges_storage().par_iter()),
            ),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.layers[layer_id].edges_storage().par_iter())
            }
            LayerIds::Multiple(ids) => {
                let ids = ids.clone();
                LayerVariants::Multiple(
                    (0..ids.len())
                        .into_par_iter()
                        .flat_map(move |i| self.layers[ids[i]].edges_storage().par_iter()),
                )
            }
        }
    }

    pub fn count(self, layers: &LayerIds) -> usize {
        match layers {
            LayerIds::None => 0,
            LayerIds::All => self.layers.par_iter().map(|layer| layer.num_edges()).sum(),
            LayerIds::One(id) => self.layers[*id].num_edges(),
            LayerIds::Multiple(ids) => ids
                .par_iter()
                .map(|layer| self.layers[*layer].num_edges())
                .sum(),
        }
    }

    pub fn exploded_count(self, layers: &LayerIds) -> usize {
        match layers {
            LayerIds::None => 0,
            LayerIds::All => self
                .layers
                .par_iter()
                .map(|layer| layer.num_temporal_edges())
                .sum(),
            LayerIds::One(id) => self.layers[*id].num_temporal_edges(),
            LayerIds::Multiple(ids) => ids
                .par_iter()
                .map(|layer| self.layers[*layer].num_temporal_edges())
                .sum(),
        }
    }
}
