use crate::{
    core::{entities::VID, storage::timeindex::TimeIndexEntry},
    db::api::view::internal::Base,
};
use raphtory_api::core::{
    entities::EID, storage::dict_mapper::MaybeNew, utils::errors::GraphError,
};

pub trait InternalDeletionOps {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError>;

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
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
    ) -> Result<MaybeNew<EID>, GraphError> {
        self.graph().internal_delete_edge(t, src, dst, layer)
    }

    #[inline]
    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph().internal_delete_existing_edge(t, eid, layer)
    }
}
