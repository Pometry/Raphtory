use crate::disk::{
    graph_impl::DiskEdge, storage_interface::edges_ref::DiskEdgesRef, DiskGraphStorage,
};
use itertools::Itertools;
use raphtory_api::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, LayerVariants, EID},
    iter::IntoDynBoxed,
};
use raphtory_core::utils::iter::GenLockedIter;
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

    pub fn into_iter_refs(self, layer_ids: LayerIds) -> impl Iterator<Item = EdgeRef> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(iter::empty()),
            LayerIds::All => LayerVariants::All(GenLockedIter::from(self.graph, |graph| {
                graph
                    .inner
                    .all_edge_ids()
                    .map(|(eid, src, dst)| EdgeRef::new_outgoing(eid, src, dst))
                    .into_dyn_boxed()
            })),
            LayerIds::One(layer_id) => {
                LayerVariants::One(GenLockedIter::from(self.graph, move |graph| {
                    graph
                        .inner
                        .layer_edge_ids(layer_id)
                        .map(|(eid, src, dst)| EdgeRef::new_outgoing(eid, src, dst))
                        .into_dyn_boxed()
                }))
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.into_iter()
                    .map(move |layer_id| {
                        GenLockedIter::from(self.graph.clone(), move |graph| {
                            graph.inner.layer_edge_ids(layer_id).into_dyn_boxed()
                        })
                    })
                    .kmerge_by(|(eid1, _, _), (eid2, _, _)| eid1 < eid2)
                    .dedup()
                    .map(move |(eid, src, dst)| EdgeRef::new_outgoing(eid, src, dst)),
            ),
        }
    }

    pub fn into_par_iter_refs(self, layer_ids: LayerIds) -> impl ParallelIterator<Item = EID> {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.graph.inner.all_edge_ids_par(layer_id))
            }
            LayerIds::All => {
                LayerVariants::All((0..self.graph.inner.num_edges()).into_par_iter().map(EID))
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                (0..self.graph.inner.num_edges())
                    .into_par_iter()
                    .map(EID)
                    .filter(move |e| {
                        ids.into_iter()
                            .any(|layer_id| self.graph.inner.edge(*e).has_layer_inner(layer_id))
                    }),
            ),
        }
    }

    pub fn get(&self, eid: EID) -> DiskEdge {
        self.graph.inner.edge(eid)
    }
}
