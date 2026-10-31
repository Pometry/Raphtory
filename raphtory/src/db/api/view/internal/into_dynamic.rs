use crate::db::api::view::{
    internal::{DynGraphArc, DynamicGraph, InternalFilter, Static},
    BoxableGraphView, StaticGraphViewOps,
};
use std::sync::Arc;

pub trait IntoDynamic: 'static {
    fn into_dynamic(self) -> DynamicGraph;
}

impl<G: StaticGraphViewOps + Static> IntoDynamic for G {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}

impl IntoDynamic for Arc<dyn BoxableGraphView> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph(self)
    }
}

/// Erase a graph view into a `DynGraphArc`. A view that is already an erased
/// `Arc` is handed back as it is instead of being boxed a second time.
pub trait IntoDynGraphArc {
    fn into_dyn_graph_arc<'graph>(self) -> DynGraphArc<'graph>
    where
        Self: 'graph;
}

impl<G: BoxableGraphView + Static> IntoDynGraphArc for G {
    #[inline]
    fn into_dyn_graph_arc<'graph>(self) -> DynGraphArc<'graph>
    where
        Self: 'graph,
    {
        Arc::new(self)
    }
}

impl<'a> IntoDynGraphArc for Arc<dyn BoxableGraphView + 'a> {
    #[inline]
    fn into_dyn_graph_arc<'graph>(self) -> DynGraphArc<'graph>
    where
        Self: 'graph,
    {
        self
    }
}

impl<G: BoxableGraphView + Static> IntoDynGraphArc for Arc<G> {
    #[inline]
    fn into_dyn_graph_arc<'graph>(self) -> DynGraphArc<'graph>
    where
        Self: 'graph,
    {
        self
    }
}

impl IntoDynGraphArc for DynamicGraph {
    #[inline]
    fn into_dyn_graph_arc<'graph>(self) -> DynGraphArc<'graph>
    where
        Self: 'graph,
    {
        self.0
    }
}

pub trait IntoDynHop: InternalFilter<'static, Graph: IntoDynamic> {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph>;
}

impl<T: InternalFilter<'static, Graph: IntoDynamic + Clone>> IntoDynHop for T {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph> {
        let graph = self.base_graph().clone().into_dynamic();
        self.apply_filter(graph)
    }
}
