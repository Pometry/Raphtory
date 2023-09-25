use crate::{
    core::{entities::VID, storage::timeindex::TimeIndexEntry, utils::errors::GraphError},
    db::api::view::internal::Base,
};

pub trait InternalDeletionOps {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError>;
}

pub trait InheritDeletionOps: Base {}

impl<G: InheritDeletionOps> DelegateDeletionOps for G
where
    G::Base: InternalDeletionOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateDeletionOps {
    type Internal: InternalDeletionOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateDeletionOps> InternalDeletionOps for G {
    #[inline(always)]
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph().internal_delete_edge(t, src, dst, layer)
    }
}
