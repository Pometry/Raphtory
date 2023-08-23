use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::api::view::internal::Base,
};
use std::sync::Arc;

pub fn extend_filter(
    old: Option<EdgeFilter>,
    filter: impl Fn(&EdgeStore, &LayerIds) -> bool + Send + Sync + 'static,
) -> EdgeFilter {
    match old {
        Some(f) => Arc::new(move |e, l| f(e, l) && filter(e, l)),
        None => Arc::new(filter),
    }
}

pub type EdgeFilter = Arc<dyn Fn(&EdgeStore, &LayerIds) -> bool + Send + Sync>;

pub trait EdgeFilterOps {
    /// Return the optional edge filter for the graph
    fn edge_filter(&self) -> Option<EdgeFilter>;
}

pub trait InheritEdgeFilterOps: Base {}

impl<G: InheritEdgeFilterOps> DelegateEdgeFilterOps for G
where
    G::Base: EdgeFilterOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateEdgeFilterOps {
    type Internal: EdgeFilterOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateEdgeFilterOps> EdgeFilterOps for G {
    fn edge_filter(&self) -> Option<EdgeFilter> {
        self.graph().edge_filter()
    }
}
