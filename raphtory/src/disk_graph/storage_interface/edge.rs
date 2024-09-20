use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID},
        storage::timeindex::TimeIndexIntoOps,
        utils::iter::GenLockedIter,
    },
    db::api::{storage::graph::edges::edge_storage_ops::{EdgeStorageIntoOps, EdgeStorageOps}, view::IntoDynBoxed},
    disk_graph::DiskGraphStorage,
};
use itertools::Itertools;
use pometry_storage::edge::Edge;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::{ops::Range, sync::Arc};

pub type DiskEdge<'a> = Edge<'a>;

#[derive(Debug, Clone)]
pub struct DiskOwnedEdge {
    graph: Arc<DiskGraphStorage>,
    eid: EID,
}

impl DiskOwnedEdge {
    pub(crate) fn new(graph: Arc<DiskGraphStorage>, eid: EID) -> Self {
        Self { graph, eid }
    }
    pub fn as_ref(&self) -> DiskEdge {
        self.graph.inner.edge(self.eid)
    }
}

impl EdgeStorageIntoOps for DiskOwnedEdge {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);

        GenLockedIter::from((self, layer_ids), |(edge, layers)| {
            Box::new(
                edge.as_ref()
                    .layer_ids_iter(layers)
                    .map(move |l| eref.at_layer(l)),
            )
        })
    }

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((self, layer_ids, eref), |(edge, layers, eref)| {
            edge.as_ref()
                .additions_iter(layers)
                .map(move |(l, a)| a.into_iter().map(move |t| eref.at(t).at_layer(l)))
                .kmerge_by(|e1, e2| e1.time() <= e2.time())
                .into_dyn_boxed()
        })
    }

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_ids = layer_ids.constrain_from_edge(eref);
        GenLockedIter::from((self, layer_ids, w), |(edge, layers, w)| {
            Box::new(
                edge.as_ref()
                    .additions_iter(layers)
                    .flat_map(move |(l, a)| {
                        a.into_range(w.clone())
                            .into_iter()
                            .map(move |t| eref.at(t).at_layer(l))
                    }),
            )
        })
   }
}
