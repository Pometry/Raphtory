use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, EID, ELID},
        storage::timeindex::TimeIndexOps,
    },
    db::api::storage::edges::edge_storage_ops::EdgeStorageIntoOps,
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_arrow::{edge::Edge, edges::Edges, graph::TemporalGraph, timestamps::TimeStamps};
use std::ops::Range;

pub type ArrowEdge<'a> = Edge<'a>;

#[derive(Debug, Clone)]
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
            edges: graph.layer(layer).edges_storage().clone(),
            eid: eid.pid(),
        }
    }
    pub fn as_ref(&self) -> ArrowEdge {
        self.edges.edge(self.eid)
    }
}

impl EdgeStorageIntoOps for ArrowOwnedEdge {
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
