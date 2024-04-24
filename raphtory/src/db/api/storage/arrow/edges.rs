use crate::{
    arrow::{edge::Edge, edges::Edges, graph::TemporalGraph, graph_fragment::TempColGraphFragment},
    core::entities::{LayerIds, EID, ELID},
    db::api::storage::layer_variants::LayerVariants,
};
use rayon::prelude::*;
use std::{iter, sync::Arc};

#[derive(Clone, Debug)]
pub struct ArrowEdges {
    layers: Arc<[TempColGraphFragment]>,
}

impl ArrowEdges {
    pub(crate) fn new(graph: &TemporalGraph) -> Self {
        Self {
            layers: graph.layers.clone(),
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

    pub fn get(&self, eid: EID, layer_id: usize) -> ArrowEdge {
        self.layers[layer_id].edge(eid)
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
            LayerIds::None => LayerVariants::None(iter::empty()),
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
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
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

pub type ArrowEdge<'a> = Edge<'a>;

pub struct ArrowOwnedEdge {
    edges: Edges,
    eid: EID,
}

impl ArrowOwnedEdge {
    pub(crate) fn new(graph: &TemporalGraph, eid: ELID) -> Self {
        let layer = eid
            .layer()
            .expect("arrow EdgeRefs should have layer always defined");
        Self {
            edges: graph.layers[layer].edges.clone(),
            eid: eid.pid(),
        }
    }
    pub fn as_ref(&self) -> ArrowEdge {
        self.edges.edge(self.eid)
    }
}
