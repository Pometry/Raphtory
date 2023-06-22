use crate::core::tgraph_shard::errors::GraphError;

pub trait InternalDeletionOps {
    fn internal_delete_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;
}

pub trait InheritDeletionOps {
    type Internal: InternalDeletionOps;
    
    fn graph(&self) -> &Self::Internal;
}

impl<G: InheritDeletionOps> InternalDeletionOps for G {
    #[inline(always)]
    fn internal_delete_edge(&self, t: i64, src: u64, dst: u64, layer: Option<&str>) -> Result<(), GraphError> {
        self.graph().internal_delete_edge(t, src, dst, layer)
    }
}
