use crate::{
    core::{tgraph2::tgraph::InnerTemporalGraph, tgraph_shard::errors::GraphError}, db::mutation_api::internal::InternalDeletionOps,
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
