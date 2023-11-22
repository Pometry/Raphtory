use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;
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

pub type EdgeFilter<'graph> = Arc<dyn Fn(&EdgeStore, &LayerIds) -> bool + Send + Sync + 'graph>;

#[enum_dispatch]
pub trait EdgeFilterOps<'graph> {
    /// Return the optional edge filter for the graph
    fn edge_filter(&self) -> Option<&EdgeFilter<'graph>>;

    /// Called by the windowed graph to get the edge filter (override if it should include more/different edges than a non-windowed graph)
    #[inline]
    fn edge_filter_window(&self) -> Option<&EdgeFilter<'graph>> {
        self.edge_filter()
    }
}

pub trait InheritEdgeFilterOps: Base {}

impl<'graph, G: InheritEdgeFilterOps> DelegateEdgeFilterOps<'graph> for G
where
    G::Base: EdgeFilterOps<'graph>,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateEdgeFilterOps<'graph> {
    type Internal: EdgeFilterOps<'graph> + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<'graph, G: DelegateEdgeFilterOps<'graph>> EdgeFilterOps<'graph> for G {
    #[inline]
    fn edge_filter(&self) -> Option<&EdgeFilter<'graph>> {
        self.graph().edge_filter()
    }
}
