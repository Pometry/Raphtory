use crate::{
    arrow::storage_interface::{edge::ArrowEdge, edges_ref::ArrowEdgesRef},
    core::entities::{LayerIds, EID},
    db::api::storage::variants::layer_variants::LayerVariants,
};
use raphtory_arrow::{graph::TemporalGraph, graph_fragment::TempColGraphFragment};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{iter, sync::Arc};

#[derive(Clone, Debug)]
pub struct ArrowEdges {
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowEdges {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            layers: graph.arc_layers().clone(),
        }
    }
    pub fn as_ref(&self) -> ArrowEdgesRef {
        ArrowEdgesRef {
            layers: &self.layers,
        }
    }

    pub fn into_iter_refs(self, layer_ids: LayerIds) -> impl Iterator<Item = (EID, usize)> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All((0..self.layers.len()).flat_map(move |layer_id| {
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e.into(), layer_id))
            })),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e.into(), layer_id)),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple((0..ids.len()).flat_map(move |i| {
                let layer_id = ids[i];
                self.layers[layer_id]
                    .all_edge_ids()
                    .map(move |e| (e.into(), layer_id))
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
                        .map(move |e| (e.into(), layer_id))
                },
            )),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.layers[layer_id]
                    .all_edge_ids_par()
                    .map(move |e| (e.into(), layer_id)),
            ),
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple((0..ids.len()).into_par_iter().flat_map(move |i| {
                    let layer_id = ids[i];
                    self.layers[layer_id]
                        .all_edge_ids_par()
                        .map(move |e| (e.into(), layer_id))
                }))
            }
        }
    }

    pub fn get(&self, eid: EID, layer_id: usize) -> ArrowEdge {
        self.layers[layer_id].edge(eid)
    }
}
