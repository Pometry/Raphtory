use crate::{
    core::entities::{LayerIds, EID},
    db::api::storage::graph::variants::layer_variants::LayerVariants,
    disk_graph::{
        storage_interface::{edge::DiskEdge, edges_ref::DiskEdgesRef},
        DiskGraphStorage,
    },
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{iter, sync::Arc};

#[derive(Clone, Debug)]
pub struct DiskEdges {
    graph: Arc<DiskGraphStorage>,
}

impl DiskEdges {
    pub(crate) fn new(graph: &DiskGraphStorage) -> Self {
        Self {
            graph: Arc::new(graph.clone()),
        }
    }

    pub fn as_ref(&self) -> DiskEdgesRef {
        DiskEdgesRef {
            graph: &self.graph.inner,
        }
    }

    pub fn into_iter_refs(self, layer_ids: LayerIds) -> impl Iterator<Item = (EID, usize)> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All((0..self.graph.inner.layers().len()).flat_map(
                move |layer_id| {
                    self.graph
                        .inner
                        .all_edge_ids(layer_id)
                        .map(move |e| (e, layer_id))
                },
            )),
            LayerIds::One(layer_id) => LayerVariants::One(
                self.graph
                    .inner
                    .all_edge_ids(layer_id)
                    .map(move |e| (e, layer_id)),
            ),
            LayerIds::Multiple(ids) => LayerVariants::Multiple((0..ids.len()).flat_map(move |i| {
                let layer_id = ids[i];
                self.graph
                    .inner
                    .all_edge_ids(layer_id)
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
            LayerIds::One(layer_id) => LayerVariants::One(
                self.graph
                    .inner
                    .all_edge_ids_par(layer_id)
                    .map(move |e| (e, layer_id)),
            ),
            LayerIds::All => LayerVariants::All(
                (0..self.graph.inner.layers().len())
                    .into_par_iter()
                    .flat_map(move |layer_id| {
                        self.graph
                            .inner
                            .all_edge_ids_par(layer_id)
                            .map(move |e| (e, layer_id))
                    }),
            ),
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple((0..ids.len()).into_par_iter().flat_map(move |i| {
                    let layer_id = ids[i];
                    self.graph
                        .inner
                        .all_edge_ids_par(layer_id)
                        .map(move |e| (e, layer_id))
                }))
            }
        }
    }

    pub fn get(&self, eid: EID) -> DiskEdge {
        self.graph.inner.edge(eid)
    }
}
