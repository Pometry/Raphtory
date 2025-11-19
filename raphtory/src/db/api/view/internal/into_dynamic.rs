use crate::db::api::view::{
    internal::{DynamicGraph, Filter, Static},
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

pub trait IntoDynHop: Filter<'static, Graph: IntoDynamic> {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph>;
}

impl<T: Filter<'static, Graph: IntoDynamic + Clone>> IntoDynHop for T {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph> {
        let graph = self.base_graph().clone().into_dynamic();
        self.apply_filter(graph)
    }
}
