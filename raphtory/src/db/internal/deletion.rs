use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, graph::tgraph::InnerTemporalGraph, LayerIds},
        storage::timeindex::{LockedLayeredIndex, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::api::{mutation::internal::InternalDeletionOps, view::internal::CoreDeletionOps},
};

impl<const N: usize> InternalDeletionOps for InnerTemporalGraph<N> {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.inner().delete_edge(t, src, dst, layer)
    }
}

impl<const N: usize> CoreDeletionOps for InnerTemporalGraph<N> {
    fn edge_deletions(&self, eref: EdgeRef, layer_ids: LayerIds) -> LockedLayeredIndex<'_> {
        let edge = self.inner().edge(eref.pid());
        edge.deletions(layer_ids).unwrap()
    }
}
