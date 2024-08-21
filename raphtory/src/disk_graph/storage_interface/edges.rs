use crate::{
    core::entities::{LayerIds, EID},
    db::api::storage::graph::variants::layer_variants::LayerVariants,
    disk_graph::storage_interface::{edge::DiskEdge, edges_ref::DiskEdgesRef},
};
use pometry_storage::{graph::TemporalGraph, graph_fragment::TempColGraphFragment};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{iter, sync::Arc};

#[derive(Clone, Debug)]
pub struct DiskEdges {
    layers: Arc<[TempColGraphFragment]>,
}

impl DiskEdges {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            layers: graph.arc_layers().clone(),
        }
    }
    pub fn as_ref(&self) -> DiskEdgesRef {
        DiskEdgesRef {
            layers: &self.layers,
        }
    }

    pub fn into_iter_refs(self, layer_ids: LayerIds) -> impl Iterator<Item = (EID, usize)> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All((0..self.layers.len()).flat_map(move |layer_id| {
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e, layer_id))
            })),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e, layer_id)),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple((0..ids.len()).flat_map(move |i| {
                let layer_id = ids[i];
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e, layer_id))
            })),
        }
    }

    pub fn into_par_iter_refs(
        self,
        layer_ids: LayerIds,
    ) -> impl ParallelIterator<Item = (EID, usize)> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All((0..self.layers.len()).into_par_iter().flat_map(
                move |layer_id| {
                    self.layers[layer_id]
                        .all_edge_ids_par()
                        .map(move |e| (e, layer_id))
                },
            )),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[layer_id]
                    .all_edge_ids_par()
                    .map(move |e| (e, layer_id)),
            ),
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple((0..ids.len()).into_par_iter().flat_map(move |i| {
                    let layer_id = ids[i];
                    self.layers[layer_id]
                        .all_edge_ids_par()
                        .map(move |e| (e, layer_id))
                }))
            }
        }
    }

    pub fn get(&self, eid: EID, layer_id: usize) -> DiskEdge {
        self.layers[layer_id].edge(eid)
    }
}
