use crate::db::api::view::{
    internal::{DynamicGraph, OneHopFilter, Static},
    StaticGraphViewOps,
};

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

pub trait IntoDynHop: OneHopFilter<'static, FilteredGraph: IntoDynamic> {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph>;
}

impl<T: OneHopFilter<'static, FilteredGraph: IntoDynamic + Clone>> IntoDynHop for T {
    fn into_dyn_hop(self) -> Self::Filtered<DynamicGraph> {
        let graph = self.current_filter().clone().into_dynamic();
        self.one_hop_filtered(graph)
    }
}
