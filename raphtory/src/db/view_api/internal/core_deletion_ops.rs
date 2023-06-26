use crate::core::edge_ref::EdgeRef;
use crate::core::tgraph_shard::LockedView;
use crate::core::timeindex::TimeIndex;
use crate::db::view_api::internal::Base;

pub trait CoreDeletionOps {
    /// Get all the deletion timestamps for an edge
    /// (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)
    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex>;
}

pub trait InheritCoreDeletionOps: Base {}

impl<G: InheritCoreDeletionOps> DelegateCoreDeletionOps for G
where
    G::Base: CoreDeletionOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreDeletionOps {
    type Internal: CoreDeletionOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreDeletionOps> CoreDeletionOps for G {
    fn edge_deletions(&self, eref: EdgeRef) -> LockedView<TimeIndex> {
        self.graph().edge_deletions(eref)
    }
}
