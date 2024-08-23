use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, ELID},
        storage::timeindex::TimeIndexOps,
    },
    db::api::storage::graph::edges::edge_storage_ops::EdgeStorageIntoOps,
};
use pometry_storage::{edge::Edge, edges::Edges, graph::TemporalGraph, timestamps::TimeStamps};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::ops::Range;

pub type DiskEdge<'a> = Edge<'a>;

#[derive(Debug, Clone)]
pub struct DiskOwnedEdge {
    edges: Edges,
    eid: EID,
}

impl DiskOwnedEdge {
    pub(crate) fn new(graph: &TemporalGraph, eid: ELID) -> Self {
        let layer = eid
            .layer()
            .expect("disk_graph EdgeRefs should have layer always defined");
        Self {
            edges: graph.layer(layer).edges_storage().clone(),
            eid: eid.pid(),
        }
    }
    pub fn as_ref(&self) -> DiskEdge {
        self.edges.edge(self.eid)
    }
}

impl EdgeStorageIntoOps for DiskOwnedEdge {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_id = self.edges.layer_id();
        layer_ids.contains(&layer_id).then_some(eref).into_iter()
    }

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_id = self.edges.layer_id();
        layer_ids
            .contains(&layer_id)
            .then(move || {
                let ts = self.edges.into_time().into_value(self.eid.0);
                let range = ts.range().clone();
                ts.zip(range)
                    .map(move |(t, s)| eref.at(TimeIndexEntry(t, s)))
            })
            .into_iter()
            .flatten()
    }

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send {
        let layer_id = self.edges.layer_id();
        layer_ids
            .contains(&layer_id)
            .then(move || {
                let array = self.edges.into_time();
                let ts: TimeStamps<TimeIndexEntry> = TimeStamps::new(array.value(self.eid.0), None);
                let times = ts.range(w).timestamps().into_owned();
                let range = times.range().clone();
                times
                    .zip(range)
                    .map(move |(t, s)| eref.at(TimeIndexEntry(t, s)))
            })
            .into_iter()
            .flatten()
    }
}
