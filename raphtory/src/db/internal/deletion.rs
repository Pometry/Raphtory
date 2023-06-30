use crate::core::storage::locked_view::LockedView;
use crate::core::storage::timeindex::TimeIndex;
use crate::core::tgraph::edges::edge_ref::EdgeRef;
use crate::core::tgraph::graph::tgraph::InnerTemporalGraph;
use crate::core::utils::errors::GraphError;
use crate::db::api::mutation::internal::InternalDeletionOps;
use crate::db::api::view::internal::CoreDeletionOps;

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

impl<const N: usize> CoreDeletionOps for InnerTemporalGraph<N> {
    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        let edge = self.edge(eref.pid());
        edge.deletions(eref.layer()).unwrap()
    }
}
