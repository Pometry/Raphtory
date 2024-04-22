use crate::{
    arrow::{edge::Edge, graph::TemporalGraph, graph_fragment::TempColGraphFragment},
    core::entities::{LayerIds, EID},
    db::api::{storage::layer_variants::LayerVariants, view::IntoDynBoxed},
};
use rayon::prelude::*;
use std::{iter, sync::Arc};

pub struct ArrowEdges {
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowEdges {
    pub fn as_ref(&self) -> ArrowEdgesRef {
        ArrowEdgesRef {
            layers: &self.layers,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ArrowEdgesRef<'a> {
    layers: &'a [TempColGraphFragment],
}

impl<'a> ArrowEdgesRef<'a> {
    pub(crate) fn new(storage: &'a TemporalGraph) -> Self {
        Self {
            layers: &storage.layers,
        }
    }

    pub fn edge(self, eid: EID, layer_id: usize) -> ArrowEdge<'a> {
        self.layers[layer_id].edge(eid)
    }

    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = ArrowEdge<'a>> {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => {
                LayerVariants::All(self.layers.iter().flat_map(|layer| layer.edges.iter()))
            }
            LayerIds::One(layer_id) => LayerVariants::One(self.layers[layer_id].edges.iter()),
            LayerIds::Multiple(ids) => {
                let ids = ids.clone();
                LayerVariants::Multiple(
                    (0..ids.len()).flat_map(move |i| self.layers[ids[i]].edges.iter()),
                )
            }
        }
    }

    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = ArrowEdge<'a>> {
        match layers {
            LayerIds::None => LayerVariants::None,
            LayerIds::All => LayerVariants::All(
                self.layers
                    .par_iter()
                    .flat_map(|layer| layer.edges.par_iter()),
            ),
            LayerIds::One(layer_id) => LayerVariants::One(self.layers[layer_id].edges.par_iter()),
            LayerIds::Multiple(ids) => {
                let ids = ids.clone();
                LayerVariants::Multiple(
                    (0..ids.len())
                        .into_par_iter()
                        .flat_map(move |i| self.layers[ids[i]].edges.par_iter()),
                )
            }
        }
    }
}

pub type ArrowEdge<'a> = Edge<'a>;

pub struct ArrowEdgesOwned {
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowEdgesOwned {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            layers: graph.layers.clone(),
        }
    }
}
