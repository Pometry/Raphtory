use crate::{
    core::{tgraph::tgraph::InnerTemporalGraph, errors::GraphError, timeindex::TimeIndex, edge_ref::EdgeRef, locked_view::LockedView}, db::{mutation_api::internal::InternalDeletionOps, view_api::internal::CoreDeletionOps},
};

impl<const N: usize> InternalDeletionOps for InnerTemporalGraph<N> {
    fn internal_delete_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.delete_edge(t, src, dst, layer)
    }
}


impl<const N:usize> CoreDeletionOps for InnerTemporalGraph<N> {
    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        let edge = self.edge(eref.pid());
        edge.deletions(eref.layer()).unwrap()
    }
}